package com.growse.lmdb_kt

import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.ByteBuffer

private val logger = KotlinLogging.logger {}

/**
 * Represents a page header http://www.lmdb.tech/doc/group__internal.html#structMDB__page
 *
 * union { pgno_t p_pgno struct MDB_page * p_next } mp_p uint16_t mp_pad uint16_t mp_flags union {
 * struct { indx_t pb_lower indx_t pb_upper } pb uint32_t pb_pages } mp_pb indx_t mp_ptrs
 *
 * @param buffer a [ByteBuffer] for the page
 * @constructor Parses the page header, determining whether the page is an overflow
 */
class PageHeader(buffer: DbMappedBuffer, private val pageNumber: UInt) {
  val storedPageNumber: Long by lazy {
    logger.trace { "page $pageNumber reading page number" }
    buffer.run {
      seek(pageNumber)
      readLong()
    }
  }
  val flags by lazy {
    logger.trace { "page $pageNumber reading flags" }
    buffer.run {
      seek(pageNumber, 8u + 2u) // 2u of padding
      flags(Page.Flags::class.java, 2u)
    }
  }

  val pagesOrRange: Either<UInt, Environment.Range> by lazy {
    buffer.run {
      val isOverflow = flags.contains(Page.Flags.OVERFLOW)
      seek(pageNumber, 12u)
      if (isOverflow) {
        Either.Left(buffer.readUInt())
      } else {
        Either.Right(Environment.Range(buffer.readUShort(), buffer.readUShort()))
      }
    }
  }

  fun numKeys(): Int =
      when (pagesOrRange) {
        is Either.Left -> 0
        is Either.Right ->
            ((pagesOrRange as Either.Right<UInt, Environment.Range>).right.lower.toShort() - 16) / 2
      }

  companion object {
    const val SIZE: UInt = 16u
  }
}
