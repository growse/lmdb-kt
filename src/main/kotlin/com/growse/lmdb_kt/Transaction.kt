package com.growse.lmdb_kt

import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

class Transaction(private val metadata: MetaDataPage64) : AutoCloseable {
	fun openDatabase(name: String = "0"): DB {
		if (name == "0") {
			return metadata.mainDb
		} else {
			TODO("Named databases not supported yet")
		}
	}

	override fun close() {
		logger.trace { "Close" }
	}
}
