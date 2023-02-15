package com.growse.lmdb_kt

import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

class Transaction(private val env: Environment, databaseName: String = "0") :
	AutoCloseable {
	private var database: DB

	init {
		database = openDatabase(databaseName)
	}

	private fun openDatabase(name: String = "0"): DB {
		if (name == "0") {
			return env.latestMetadataPage().mainDb.also { database = it }
		} else {
			TODO("Named databases not supported yet")
		}
	}

	fun get(key: ByteArray): Result<ByteArray> {
		logger.debug { "Get key ${key.toHex()} from database" }
		return if (database.rootPageNumber == -1L) {
			Result.failure(Exception("Key not found"))
		} else {
			database.buffer.getPage(database.rootPageNumber.toUInt()).get(key)
		}
	}

	/**
	 * Dumps all keys/values out
	 *
	 * @return a map of keys (as strings) and values
	 */
	fun dump(): Map<String, ByteArray> {
		logger.debug { "Dump database" }
		if (database.rootPageNumber == -1L) {
			return emptyMap()
		}
		return database.buffer.getPage(database.rootPageNumber.toUInt()).dump()
	}

	override fun close() {
		logger.trace { "Close. Noop" }
	}
}
