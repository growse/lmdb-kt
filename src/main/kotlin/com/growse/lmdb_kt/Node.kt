package com.growse.lmdb_kt

import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.ByteBuffer
import java.util.EnumSet

private val logger = KotlinLogging.logger {}

abstract class Node(private val buffer: DbMappedBuffer, pageNumber: UInt, addressInPage: UInt) {
  protected val lo: UShort by lazy {
    buffer.run {
      seek(pageNumber, addressInPage)
      readUShort()
    }
  }
  protected val hi: UShort by lazy {
    buffer.run {
      seek(pageNumber, addressInPage + 2u)
      readUShort()
    }
  }
  protected val flags: EnumSet<Flags> by lazy {
    buffer.run {
      seek(pageNumber, addressInPage + 4u)
      flags(Flags::class.java, 2u)
    }
  }
  protected val keySize: UShort by lazy {
    buffer.run {
      seek(pageNumber, addressInPage + 6u)
      readUShort()
    }
  }
  val key: ByteBuffer = buffer.slice(pageNumber, (addressInPage.toInt() + 8), keySize.toInt())

  fun copyKeyBytes(): ByteArray {
    return ByteArray(keySize.toInt()).apply(key::get).also {
      logger.trace { "Pos: ${buffer.position()}" }
    }
  }

  /** Node flags http://www.lmdb.tech/doc/group__mdb__node.html */
  enum class Flags(val idx: Int) {
    BIGDATA(0),
    SUBDATA(1),
    DUPDATA(2),
  }
}
