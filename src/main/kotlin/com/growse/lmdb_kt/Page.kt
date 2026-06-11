package com.growse.lmdb_kt

import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.ByteBuffer
import java.util.EnumSet

private val logger = KotlinLogging.logger {}

interface Page {
  val buffer: DbMappedBuffer
  val number: UInt

  /**
   * Dumps the keys/values from this page. Walks through to child pages. Copies data from the
   * buffer.
   *
   * @return a map of keys/values
   */
  fun dump(): Map<ByteArrayKey, ByteArray>

  /**
   * Streams all key/value pairs from this page as read-only [ByteBuffer] pairs. Walks through to
   * child pages without materialising any intermediate collections.
   */
  fun scan(): Sequence<Pair<ByteBuffer, ByteBuffer>>

  /**
   * Gets a read-only buffer view on the value stored against the requested key on this page.
   *
   * @param key to fetch the value for
   * @return a read-only buffer for the value stored against the key on this page
   */
  fun getBuffer(key: ByteArray): Result<ByteBuffer>

  /**
   * Gets a copy on the value stored against the requested key on this page
   *
   * @param key to fetch the value for
   * @return a copy of the value stored against the key on this page
   */
  fun get(key: ByteArray): Result<ByteArray> = getBuffer(key).map(ByteBuffer::copyRemainingBytes)

  /** Page flags http://www.lmdb.tech/doc/group__mdb__page.html */
  enum class Flags(val idx: Int) {
    BRANCH(0),
    LEAF(1),
    OVERFLOW(2),
    META(3),
    DIRTY(4),
    LEAF2(5),
    SUBP(6),
    LOOSE(14),
    KEEP(15),
  }

  class UnsupportedPageTypeException(private val flags: EnumSet<Flags>) : Throwable() {
    override fun toString(): String {
      return "${super.toString()} : $flags"
    }
  }

  class KeyNotFoundInPage(private val key: ByteArray, private val pageNumber: UInt) : Throwable() {
    override fun toString(): String {
      return "Key ${key.toPrintableString()} not found on page $pageNumber"
    }
  }
}
