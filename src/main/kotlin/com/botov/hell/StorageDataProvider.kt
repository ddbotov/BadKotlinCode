package com.botov.hell

import com.sun.org.slf4j.internal.LoggerFactory
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query
import ru.vtb.msa.afl.appmanager.dto.BackupMetadata
import ru.vtb.msa.afl.appmanager.dto.CacheRecord
import ru.vtb.msa.afl.appmanager.dto.DataRecord
import ru.vtb.msa.afl.appmanager.dto.StorageOperationType.GET_VALUE
import ru.vtb.msa.afl.appmanager.dto.StorageOperationType.PUT_VALUE
import ru.vtb.msa.afl.appmanager.dto.entity.CacheBackupEntity
import ru.vtb.msa.afl.appmanager.util.bindIfExist
import ru.vtb.msa.cache.manager.CacheManager
import ru.vtb.msa.cache.manager.getOne
import ru.vtb.msa.cache.serialization.ValueSerializer
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant

class StorageDataProvider(
    val shardName: String,
    val cacheManager: CacheManager,
    private val r2dbcTemplate: R2dbcEntityTemplate,
    private val ttlDurationMinutes: Long
) {

    suspend fun <T> getOne(key: String, clazz: Class<T>): DataRecord<T>? =
        getRecord(key) { deserialize(it, clazz) }

    suspend fun <T> getAll(keys: List<String>, clazz: Class<T>): List<DataRecord<T>> =
        getAllRecords(keys) { deserialize(it, clazz) }

    suspend fun <T> put(key: String, value: T) {
        val serializedValue = cacheManager.serializer.serialize(value)
        putToDatabase(key, serializedValue)
        val existed = getFromDatabase(key).first()
        putToCache(key, CacheRecord(existed.value, existed.metadata))
    }
    fun <T> deserialize(value: kotlin.String, toClass: java.lang.Class<T>): T =
        cacheManager.serializer.deserialize(value, toClass)

    suspend fun putStringValue(key: String, value: String) {
        putToDatabase(key, value)
        val existed = getFromDatabase(key).first()
        putToCache(key, CacheRecord(existed.value, existed.metadata))
    }

    suspend fun getFromDatabaseByMdmId(key: String): List<CacheBackupEntity> = coroutineScope {
        val entities = mutableListOf<Flow<CacheBackupEntity>>()

        monitoringService.notifyDatabaseOperation(shardName, GET_VALUE)

        entities += withContext(Dispatchers.Default) {
            r2dbcTemplate
                .select(CacheBackupEntity::class.java)
                .from("cache_backup")
                .matching(Query.query(where("key1").`is`(key)))
                .all()
                .asFlow()
        }

        monitoringService.notifyDatabaseOperation(shardName, GET_VALUE)
        entities += withContext(Dispatchers.Default) {
            r2dbcTemplate
                .select(CacheBackupEntity::class.java)
                .from("cache_backup_composite")
                .matching(Query.query(where("key1").`is`(key)))
                .all()
                .asFlow()
        }

        return@coroutineScope entities.merge().toList()
    }

    suspend fun <T> findBySystem(system: String, interval: ClosedRange<Instant>, clazz: Class<T>): List<DataRecord<T>> {
        val criteria = where("updated_at").greaterThanOrEquals(Timestamp.from(interval.start))
            .and("updated_at").lessThanOrEquals(Timestamp.from(interval.endInclusive))
            .and("value::json ->> 'sourceSystem'").`is`(system)

        return r2dbcTemplate
            .select(CacheBackupEntity::class.java)
            .from("cache_backup_composite")
            .matching(Query.query(criteria))
            .all()
            .map { DataRecord(cacheManager.serializer.deserialize(it.value, clazz), it.metadata, it.compositeKey) }
            .collectList()
            .awaitSingle()
    }

    private suspend fun <T> getRecord(key: String, action: ValueSerializer.(String) -> T): DataRecord<T>? {
        val cached = getFromCache(key)
        return when {
            cached?.value != null -> {
                val obj = action.invoke(cacheManager.serializer, cached.value)
                DataRecord(obj, cached.metadata, key.compositeKey)
            }

            else -> {
                val record = getFromDatabase(key).firstOrNull() ?: return null
                val obj = action.invoke(cacheManager.serializer, record.value)
                restoreCache(record)
                DataRecord(obj, record.metadata, record.key1 to record.key2)
            }
        }
    }

    private suspend fun <T> getAllRecords(
        keys: List<String>,
        action: ValueSerializer.(String) -> T
    ): List<DataRecord<T>> =
        coroutineScope {
            if (keys.isEmpty()) return@coroutineScope emptyList()
            val databaseKeys = mutableListOf<String>()
            val cacheRecords = mutableListOf<DataRecord<T>>()
            val databaseRecords = mutableListOf<DataRecord<T>>()

            //пытаемся найти записи в кеше
            keys.chunked(100).map { localKeys ->
                localKeys.map { key ->
                    async {
                        val record = getFromCache(key)
                        if (record?.value == null) {
                            databaseKeys += key; return@async
                        }
                        val value = action.invoke(cacheManager.serializer, record.value)
                        cacheRecords += DataRecord(value, record.metadata, key.compositeKey)
                    }
                }
                    .awaitAll()
            }

            //те записи которые не смогли найти в кеше - достаем из базы большим запросов (а не по одной)
            val arrayOfDatabaseKeys = databaseKeys.toTypedArray()
            getFromDatabase(*arrayOfDatabaseKeys).forEach {
                val value = action.invoke(cacheManager.serializer, it.value)
                databaseRecords += DataRecord(value, it.metadata, it.key1 to it.key2)
                restoreCache(it)
            }

            return@coroutineScope cacheRecords + databaseRecords
        }

    private suspend fun getFromCache(key: String): CacheRecord<String>? {
        return try {
            monitoringService.notifyCacheOperation(shardName, GET_VALUE)
            cacheManager.getOne<CacheRecord<String>>(key)
        } catch (ex: Exception) {
            logger.error("Exception while accessing cache shard '$shardName', key = $key", ex)
            null
        }
    }

    private suspend fun getFromDatabase(vararg keys: String): List<CacheBackupEntity> = coroutineScope {
        val entities = mutableListOf<Flow<CacheBackupEntity>>()
        val compositeKeys = keys.map { it.compositeKey }
        val doubleKeys = compositeKeys.filter { it.second != null }
        val singleKeys = compositeKeys.filter { it.second.isNullOrEmpty() }.mapTo(HashSet()) { it.first }

        if (singleKeys.isNotEmpty()) {
            monitoringService.notifyDatabaseOperation(shardName, GET_VALUE)

            entities += singleKeys.chunked(50).map { chunkedList ->
                async {
                    r2dbcTemplate
                        .select(CacheBackupEntity::class.java)
                        .from("cache_backup")
                        .matching(Query.query(where("key1").`in`(chunkedList)))
                        .all()
                        .asFlow()
                }
            }
                .awaitAll()
        }

        if (doubleKeys.isNotEmpty()) {
            monitoringService.notifyDatabaseOperation(shardName, GET_VALUE)

            entities += doubleKeys.chunked(100).map { chunkedList ->
                async {
                    val criteria = chunkedList
                        .map { where("key1").`is`(it.first).and("key2").`is`(it.second!!) }
                        .reduce { acc, criteria -> acc.or(criteria) }

                    r2dbcTemplate
                        .select(CacheBackupEntity::class.java)
                        .from("cache_backup_composite")
                        .matching(Query.query(criteria))
                        .all()
                        .asFlow()
                }
            }
                .awaitAll()
        }

        return@coroutineScope entities.merge().toList()
    }

    suspend fun getFromDatabaseCacheBackup(singleKey: String): CacheBackupEntity = coroutineScope {
        monitoringService.notifyDatabaseOperation(shardName, GET_VALUE)
        val entity = async {
            r2dbcTemplate
                .select(CacheBackupEntity::class.java)
                .from("cache_backup")
                .matching(Query.query(where("key1").`is`(singleKey)))
                .all()
                .asFlow()
        }.await()
        return@coroutineScope entity.first()
    }
    suspend fun getFromDatabaseCacheBackupComposite(key1: String): List<CacheBackupEntity> = coroutineScope {
        monitoringService.notifyDatabaseOperation(shardName, GET_VALUE)
        val entities = mutableListOf<Flow<CacheBackupEntity>>()
        entities +=
            async {
                r2dbcTemplate
                    .select(CacheBackupEntity::class.java)
                    .from("cache_backup_composite")
                    .matching(Query.query(where("key1").`is`(key1)))
                    .all()
                    .asFlow()
            }.await()
        return@coroutineScope entities.merge().toList()
    }

    private suspend fun putToCache(key: String, record: CacheRecord<String>) {
        try {
            monitoringService.notifyCacheOperation(shardName, PUT_VALUE)
            cacheManager.put(key = key, value = record, ttl = Duration.ofMinutes(ttlDurationMinutes))
        } catch (ex: Exception) {
            logger.error("Exception while updating cache shard '$shardName' key = $key", ex)
            throw ex
        }
    }

    private suspend fun restoreCache(entity: CacheBackupEntity) {
        GlobalScope.launch(Dispatchers.IO + MDCContext()) {
            val record = CacheRecord(entity.value, entity.metadata)
            val key = if (entity.key2 == null) entity.key1 else "${entity.key1}:${entity.key2}"
            putToCache(key, record)
        }
    }

    private suspend fun putToDatabase(key: String, value: String) {
        val compositeKey = key.compositeKey
        val query = when {
            compositeKey.second.isNullOrEmpty() -> {
                """
                INSERT INTO cache_backup (key1, value)
                VALUES (:key1, :value)
                ON CONFLICT ON CONSTRAINT cache_backup_pkey
                DO UPDATE SET value = (select jsonb_agg(items.val) from (
                                        select jsonb_array_elements(cache_backup.value::jsonb) as val
                                        union
                                        select jsonb_array_elements(excluded.value::jsonb) as val) items),
                    updated_at = :updated_at, to_delete = false
                """
            } else -> {
                """
                INSERT INTO cache_backup_composite (key1, key2, value)
                VALUES (:key1, :key2, :value)
                ON CONFLICT ON CONSTRAINT cache_backup_composite_pkey
                DO UPDATE SET value = :value, updated_at = :updated_at, to_delete = false
                """
            }
        }

        r2dbcTemplate.databaseClient.sql(query)
            .bind("key1", compositeKey.first)
            .bind("value", value)
            .bind("updated_at", Timestamp.from(Instant.now()))
            .bindIfExist("key2", compositeKey.second)
            .then()
            .awaitFirstOrNull()
    }

    private val CacheBackupEntity.metadata: BackupMetadata
        get() = BackupMetadata(createdAt, updatedAt)

    private val CacheBackupEntity.compositeKey: Pair<String, String?>
        get() = key1 to key2

    private val String.compositeKey: Pair<String, String?>
        get() {
            val keys: List<String> = this.split(compositeKeyDelimiter)
            return if (keys.size == 1) keys[0] to null else keys[0] to keys[1]
        }

    companion object {
        private const val compositeKeyDelimiter = ":"
        private val logger = LoggerFactory.getLogger(Integer.class) // bad
    }
}