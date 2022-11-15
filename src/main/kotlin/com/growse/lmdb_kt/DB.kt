package com.growse.lmdb_kt

import java.nio.ByteBuffer
import java.util.*

/**
 * Represents a single database
 * http://www.lmdb.tech/doc/group__internal.html#structMDB__db
 *  uint32_t 	md_pad
 *  uint16_t 	md_flags
 *  uint16_t 	md_depth
 *  pgno_t 	    md_branch_pages
 *  pgno_t 	    md_leaf_pages
 *  pgno_t 	    md_overflow_pages
 *  size_t 	    md_entries
 *  pgno_t 	    md_root
 */
data class DB(
	private val buffer: ByteBufferWithPageSize,
	val pad: Int,
	val flags: EnumSet<Flags>,
	val depth: Short,
	val branchPages: Long,
	val leafPages: Long,
	val overflowPages: Long,
	val entries: Long,
	val rootPageNumber: Long
) {
	constructor(buffer: ByteBufferWithPageSize) : this(
		buffer = buffer,
		pad = buffer.buffer.int,
		flags = flagsFromBuffer(Flags::class.java, buffer.buffer, 2u),
		depth = buffer.buffer.short,
		branchPages = buffer.buffer.long,
		leafPages = buffer.buffer.long,
		overflowPages = buffer.buffer.long,
		entries = buffer.buffer.long,
		rootPageNumber = buffer.buffer.long
	)

	/**
	 * Dumps all keys/values out
	 *
	 * @return a map of keys (as strings) and values
	 */
	fun dump(): Map<String, ByteArray> {
		if (rootPageNumber == -1L) {
			return emptyMap()
		}
		return buffer.getPage(rootPageNumber.toUInt()).dump()
	}

	/**
	 * Database flags
	 * http://www.lmdb.tech/doc/group__mdb__dbi__open.html
	 */
	enum class Flags(val _idx: Int) {
		REVERSEKEY(0),
		DUPSORT(1),
		INTEGERKEY(2),
		DUPFIXED(3),
		INTEGERDUP(4),
		REVERSEDUP(5),
		CREATE(14)
	}
}
