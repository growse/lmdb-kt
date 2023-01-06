package com.growse.lmdb_kt

import mu.KotlinLogging
import java.util.*

private val logger = KotlinLogging.logger {}

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
	val buffer: DbMappedBuffer,
	val pad: Int,
	val flags: EnumSet<Flags>,
	val depth: Short,
	val branchPages: Long,
	val leafPages: Long,
	val overflowPages: Long,
	val entries: Long,
	val rootPageNumber: Long
) {
	constructor(buffer: DbMappedBuffer) : this(
		buffer = buffer,
		pad = buffer.readInt(),
		flags = buffer.flags(Flags::class.java, 2u),
		depth = buffer.readShort(),
		branchPages = buffer.readLong(),
		leafPages = buffer.readLong(),
		overflowPages = buffer.readLong(),
		entries = buffer.readLong(),
		rootPageNumber = buffer.readLong()
	)

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
