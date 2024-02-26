package com.botov.hell

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query
import org.springframework.r2dbc.core.DatabaseClient
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant

data class CacheRecord<T>(
    val value: T?,
    val metadata: BackupMetadata
)

data class BackupMetadata(
    val createdAt: Instant,
    val updatedAt: Instant
)

data class DataRecord<T>(
    val value: T,
    val metadata: BackupMetadata,
    val compositeKey: Pair<String, String?>
)

data class CacheBackupEntity(
    val key1: String,
    val key2: String?,
    val value: String,
    val createdAt: Instant,
    val updatedAt: Instant
)

interface CacheManager {
    fun <T> getOne(key: String): T?
    fun put(key: String, value: CacheRecord<String>, ttl: Duration?)
    fun <T> deserialize(it: String, clazz: Class<T>): T
    fun <T> serialize(value: T): String
}

fun DatabaseClient.GenericExecuteSpec.bindIfExist(name: String, value: Any?): DatabaseClient.GenericExecuteSpec =
    if (value != null) bind(name, value) else this

class StorageDataProvider(
    val shardName: String,
    val cacheManager: CacheManager,
    private val r2dbcTemplate: R2dbcEntityTemplate,
    private val ttlDurationMinutes: Long
) {

    suspend fun <T> put(key: String, value: T) {
        val serializedValue = cacheManager.serialize(value)
        putToDatabase(key, serializedValue)
        val existed = getFromDatabase(key).first()
        putToCache(key, CacheRecord(existed.value, existed.metadata))
    }

    suspend fun <T> getOne(key: String, clazz: Class<T>): DataRecord<T>? =
        getRecord(key) { cacheManager.deserialize(it, clazz) }

    private suspend fun <T> getRecord(key: String, action: (String) -> T): DataRecord<T>? {
        val cached = getFromCache(key)
        return when {
            cached?.value != null -> {
                val obj = action.invoke(cached.value)
                DataRecord(obj, cached.metadata, key.compositeKey)
            }

            else -> {
                val record = getFromDatabase(key).firstOrNull() ?: return null
                val obj = action.invoke(record.value)
                restoreCache(record)
                DataRecord(obj, record.metadata, record.key1 to record.key2)
            }
        }
    }

    private suspend fun getFromCache(key: String): CacheRecord<String>? {
        return try {
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

    private suspend fun putToCache(key: String, record: CacheRecord<String>) {
        try {
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

    private val String.compositeKey: Pair<String, String?>
        get() {
            val keys: List<String> = this.split(compositeKeyDelimiter)
            return if (keys.size == 1) keys[0] to null else keys[0] to keys[1]
        }

    companion object {
        private const val compositeKeyDelimiter = ":"
        private val logger = LoggerFactory.getLogger(StorageDataProvider.javaClass)
    }
}
