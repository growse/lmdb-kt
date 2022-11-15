package com.growse.lmdb_kt

/**
 * MDB_meta
 * uint32_t 	        mm_magic
 * uint32_t 	        mm_version
 * void * 	            mm_address
 * size_t 	            mm_mapsize
 * MDB_db 	            mm_dbs [CORE_DBS] (CORE_DBS   2 - Number of DBs in metapage (free and main) - also hardcoded elsewhere)
 * pgno_t 	            mm_last_pg
 * volatile txnid_t 	mm_txnid
 */
data class MetaDataPage64(
	override val buffer: ByteBufferWithPageSize,
	override val number: UInt,
	val magic: UInt,
	val version: UInt,
	val address: ULong,
	val mapSize: ULong,
	val freeDb: DB,
	val mainDb: DB,
	val lastPage: ULong,
	val txnId: ULong
) : Page {
	constructor(byteBufferWithPageSize: ByteBufferWithPageSize, number: UInt) : this(
		buffer = byteBufferWithPageSize,
		number = number,
		magic = byteBufferWithPageSize.buffer.int.toUInt(),
		version = byteBufferWithPageSize.buffer.int.toUInt(),
		address = byteBufferWithPageSize.buffer.long.toULong(),
		mapSize = byteBufferWithPageSize.buffer.long.toULong(),
		freeDb = DB(byteBufferWithPageSize),
		mainDb = DB(byteBufferWithPageSize),
		lastPage = byteBufferWithPageSize.buffer.long.toULong(),
		txnId = byteBufferWithPageSize.buffer.long.toULong()
	)
}
