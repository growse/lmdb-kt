package com.growse.lmdb_kt

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * MDB_meta uint32_t mm_magic uint32_t mm_version void * mm_address size_t mm_mapsize MDB_db mm_dbs
 * [CORE_DBS] (CORE_DBS 2 - Number of DBs in metapage (free and main) - also hardcoded elsewhere)
 * pgno_t mm_last_pg volatile txnid_t mm_txnid
 */
data class MetaDataPage64(
	override val buffer: DbMappedBuffer,
	override val number: UInt,
) : Page {
	val magic: UInt by lazy {
		buffer.run {
			seek(number, PageHeader.SIZE)
			readUInt()
		}
	}
	val version: UInt by lazy {
		buffer.run {
			seek(number, PageHeader.SIZE + 4u)
			readUInt()
		}
	}
	val address: ULong by lazy {
		buffer.run {
			seek(number, PageHeader.SIZE + 8u)
			readULong()
		}
	}
	val mapSize: ULong by lazy {
		buffer.run {
			seek(number, PageHeader.SIZE + 16u)
			readULong()
		}
	}

	val freeDb: DB by lazy {
		logger.trace { "Reading freeDb" }
		buffer.run {
			seek(number, PageHeader.SIZE + 24u)
			DB(buffer)
		}
	}
	val mainDb: DB by lazy {
		logger.trace { "Reading mainDb" }
		buffer.run {
			seek(number, PageHeader.SIZE + 24u + DB.SIZE)
			DB(buffer)
		}
	}
	val lastPage: ULong by lazy {
		buffer.run {
			seek(number, PageHeader.SIZE + 24u + DB.SIZE + DB.SIZE)
			readULong()
		}
	}

	val txnId: ULong by lazy {
		buffer.run {
			seek(number, PageHeader.SIZE + 24u + DB.SIZE + DB.SIZE + 8u)
			readULong()
		}
	}

	override fun dump(): Map<ByteArrayKey, ByteArray> {
		throw AssertionError("Can't dump a metadatapage page directly")
	}

	override fun get(key: ByteArray): Result<ByteArray> = Result.failure(
		Exception("Can't get a value from a Metadata page"),
	)
}
