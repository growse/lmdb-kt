package com.growse.lmdb_kt

import java.nio.ByteBuffer
import java.util.*

/**
 * Represents a page header
 * http://www.lmdb.tech/doc/group__internal.html#structMDB__page
 *
 *  union {
 *      pgno_t   p_pgno
 *      struct MDB_page *   p_next
 *  } 	mp_p
 *  uint16_t 	mp_pad
 *  uint16_t 	mp_flags
 *  union {
 *      struct {
 *          indx_t   pb_lower
 *          indx_t   pb_upper
 *      }   pb
 *      uint32_t   pb_pages
 *  } 	mp_pb
 *  indx_t 	mp_ptrs
 *
 * @constructor
 * Parses the page header, determining whether or not the page is an overflow
 *
 * @param buffer a [ByteBuffer] for the page
 */
class PageHeader(buffer: ByteBuffer) {
	val pageNumber: Long
	val padding: UShort
	val flags: EnumSet<Page.Flags>
	val pagesOrRange: Either<UInt, Environment.Range>

	companion object {
		const val SIZE: UInt = 16u
	}

	init {
		pageNumber = buffer.long
		padding = buffer.short.toUShort()
		flags = flagsFromBuffer(Page.Flags::class.java, buffer, 2u)
		pagesOrRange = if (flags.contains(Page.Flags.OVERFLOW)) {
			Either.Left(buffer.int.toUInt())
		} else {
			Either.Right(Environment.Range(buffer.short.toUShort(), buffer.short.toUShort()))
		}
	}

	fun numKeys(): Int = when (pagesOrRange) {
		is Either.Left -> 0
		is Either.Right -> (pagesOrRange.right.lower.toShort() - 16) / 2
	}
}
