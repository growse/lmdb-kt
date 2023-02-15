package com.growse.lmdb_kt

/**
 * MDB_meta uint32_t mm_magic uint32_t mm_version void * mm_address size_t mm_mapsize MDB_db mm_dbs
 * [CORE_DBS] (CORE_DBS 2 - Number of DBs in metapage (free and main) - also hardcoded elsewhere)
 * pgno_t mm_last_pg volatile txnid_t mm_txnid
 */
data class MetaDataPage64(
	override val buffer: DbMappedBuffer,
	override val number: UInt,
	val magic: UInt,
	val version: UInt,
	val address: ULong,
	val mapSize: ULong,
	val freeDb: DB,
	val mainDb: DB,
	val lastPage: ULong,
	val txnId: ULong,
) : Page {
	constructor(
		dbMappedBuffer: DbMappedBuffer,
		number: UInt,
	) : this(
		buffer = dbMappedBuffer,
		number = number,
		magic = dbMappedBuffer.readUInt(),
		version = dbMappedBuffer.readUInt(),
		address = dbMappedBuffer.readULong(),
		mapSize = dbMappedBuffer.readULong(),
		freeDb = DB(dbMappedBuffer),
		mainDb = DB(dbMappedBuffer),
		lastPage = dbMappedBuffer.readULong(),
		txnId = dbMappedBuffer.readULong(),
	)

	override fun dump(): Map<String, ByteArray> {
		throw AssertionError("Can't dump a metadatapage page directly")
	}

	override fun get(key: ByteArray): Result<ByteArray> = Result.failure(
		Exception("Can't get a value from a Metadata page")
	)
}
