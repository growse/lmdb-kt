package com.growse.lmdb_kt

import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.ByteBuffer

private val logger = KotlinLogging.logger {}

class Transaction(private val env: Environment, databaseName: String = "0") : AutoCloseable {
  private var database: DB

  init {
    logger.trace { "Creating transaction" }
    database = openDatabase(databaseName)
  }

  private fun openDatabase(name: String = "0"): DB {
    logger.trace { "open database $name" }
    if (name == "0") {
      return env.latestMetadataPage().mainDb.also { database = it }
    } else {
      TODO("Named databases not supported yet")
    }
  }

  fun getBuffer(key: ByteArray): Result<ByteBuffer> {
    logger.debug { "Get key ${key.toPrintableString()} from database" }
    return if (database.rootPageNumber == -1L) {
      Result.failure(Exception("Key not found"))
    } else {
      database.buffer.getPage(database.rootPageNumber.toUInt()).getBuffer(key)
    }
  }

  fun get(key: ByteArray): Result<ByteArray> = getBuffer(key).map(ByteBuffer::copyRemainingBytes)

  /**
   * Streams all key/value pairs in the database as read-only [ByteBuffer] pairs. Zero-copy: entries
   * are views into the mapped database file and are valid for the lifetime of this [Environment].
   *
   * @return a lazy [Sequence] of (key, value) buffer pairs
   */
  fun scan(): Sequence<Pair<ByteBuffer, ByteBuffer>> {
    logger.debug {
      "Scan database. Starting at root page number ${database.rootPageNumber.toUInt()}"
    }
    if (database.rootPageNumber == -1L) return emptySequence()
    return database.buffer.getPage(database.rootPageNumber.toUInt()).scan()
  }

  /**
   * Dumps all keys/values out
   *
   * @return a map of keys (as strings) and values
   */
  fun dump(): Map<ByteArrayKey, ByteArray> {
    logger.debug {
      "Dump database. Starting at root page number ${database.rootPageNumber.toUInt()}"
    }
    if (database.rootPageNumber == -1L) {
      return emptyMap()
    }
    return database.buffer.getPage(database.rootPageNumber.toUInt()).dump()
  }

  override fun close() {
    logger.trace { "Close. Noop" }
  }
}
