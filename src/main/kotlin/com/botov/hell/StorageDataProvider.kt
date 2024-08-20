package com.botov.hell

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.slf4j.MDCContext
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query
import org.springframework.r2dbc.core.DatabaseClient

 data class DataRecord(
    val value: String,
    val compositeKey: Pair<String, String?>
)

class CacheBackupEntity(
    val key1: String,
    val key2: String?,
    val value: String
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        other as CacheBackupEntity

        if (key1 != other.key1) return false
        if (key2 != other.key2) return false
        if (value != other.value) return false
        return true
    }
}

interface CacheManager {
    fun getOne(key: String): String
    fun put(key: String, value: String)
    //fun getAll(keys: Set<String>): Set<String>
}

class StorageDataProvider(
    val cacheManager: CacheManager,
    private val r2dbcTemplate: R2dbcEntityTemplate
) {

    suspend fun <T> put(key: String, value: T) {
        putToDatabase(key, value.toString())
        val existed = getFromDatabase(key).first()
        putToCache(key, existed.value)
    }

    suspend fun <T> getOne(key: String): DataRecord? {
        val cached = getFromCache(key)
        return when {
            cached != null -> {
                DataRecord(cached, key.compositeKey)
            }

            else -> {
                val record = getFromDatabase(key).firstOrNull() ?: return null
                restoreCache(record)
                DataRecord(record.value, record.key1 to record.key2)
            }
        }
    }

    private suspend fun putToCache(key: String, record: String) {
        try {
            cacheManager.put(key = key, value = record)
        } catch (ex: Exception) {
            println("Exception while updating cache key = $key")
            throw ex
        }
    }

    private suspend fun getFromCache(key: String): String? {
        return try {
            cacheManager.getOne(key)
        } catch (ex: Exception) {
            println("Exception while accessing cache key = $key")
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

    private suspend fun restoreCache(entity: CacheBackupEntity) {
        GlobalScope.launch(Dispatchers.IO + MDCContext()) {
            val key = if (entity.key2 == null) entity.key1 else "${entity.key1}:${entity.key2}"
            putToCache(key, entity.value)
        }
    }

    private suspend fun putToDatabase(key: String, value: String) {
        val compositeKey = key.compositeKey
        val query = when {
            compositeKey.second.isNullOrEmpty() -> {
                """
                INSERT INTO cache_backup (key1, value)
                VALUES (:key1, :value)
                """
            } else -> {
                """
                INSERT INTO cache_backup_composite (key1, key2, value)
                VALUES (:key1, :key2, :value)
                """
            }
        }

        r2dbcTemplate.databaseClient.sql(query)
            .bind("key1", compositeKey.first)
            .bind("value", value)
            .bindIfExist("key2", compositeKey.second)
            .then()
            .awaitFirstOrNull()
    }

    fun DatabaseClient.GenericExecuteSpec.bindIfExist(name: String, value: Any?): DatabaseClient.GenericExecuteSpec =
        if (value != null) bind(name, value) else this

    private val String.compositeKey: Pair<String, String?>
        get() {
            val keys: List<String> = this.split(":")
            return if (keys.size == 1) keys[0] to null else keys[0] to keys[1]
        }

}
