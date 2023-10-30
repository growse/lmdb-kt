package com.growse.lmdb_kt

import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.ByteBuffer

private val logger = KotlinLogging.logger {}

/**
 * A Leaf page contains a bunch of [LeafNode]s.
 *
 * @property pageHeader the page header
 * @property nodes a list of [LeafNode]
 */
data class LeafPage(
    override val buffer: DbMappedBuffer,
    override val number: UInt,
    val pageHeader: PageHeader,
) : Page {
  constructor(
      pageHeader: PageHeader,
      dbMappedBuffer: DbMappedBuffer,
      number: UInt,
  ) : this(buffer = dbMappedBuffer, number = number, pageHeader)

  private val nodes: List<LeafNode>
    get() {
      buffer.seek(number, PageHeader.SIZE)
      logger.trace { "Page $number has ${pageHeader.numKeys()} keys" }
      return IntRange(1, pageHeader.numKeys())
          .map { buffer.readShort().also { logger.trace { "key at position $it" } } }
          .map { LeafNode(buffer, number, it.toUInt()) }
    }

  override fun get(key: ByteArray): Result<ByteArray> =
      nodes
          .also {
            logger.trace { "Key Get. Looking for ${key.toPrintableString()} on LeafPage $number" }
          }
          .firstOrNull { it.copyKeyBytes().contentEquals(key) }
          .run {
            if (this == null) {
              Result.failure(Page.KeyNotFoundInPage(key, number))
            } else {
              Result.success(valueBytes())
            }
          }

  override fun dump(): Map<ByteArrayKey, ByteArray> =
      logger
          .trace { "Dump leaf page ${this.number}" }
          .run {
            nodes.associate { leafNode ->
              when (leafNode.value) {
                // It's an in-line value
                is Either.Left -> {
                  logger.trace {
                    "Leaf node ${leafNode.copyKeyBytes().toPrintableString()} has inline value size" +
                        " ${leafNode.valueSize} content=${leafNode.valueBytes().toPrintableString()}"
                            .also {
                              leafNode.key.rewind()
                              (leafNode.value as Either.Left<ByteBuffer, *>).left.rewind()
                            }
                  }
                  ByteArray(leafNode.key.limit()).apply(leafNode.key::get).toByteArrayKey() to
                      ByteArray(leafNode.valueSize.toInt())
                          .apply(
                              (leafNode.value as Either.Left<ByteBuffer, *>).left::get,
                          )
                }

                // It's an overflow value
                is Either.Right -> {
                  val overflowPage =
                      buffer.getPage(
                          (leafNode.value as Either.Right<ByteBuffer, Long>).right.toUInt(),
                      )
                  logger.trace {
                    "Leaf node ${leafNode.copyKeyBytes().toPrintableString()} " +
                        "points at an overflow page at $overflowPage".also { leafNode.key.rewind() }
                  }
                  assert(overflowPage is OverflowPage)
                  leafNode.copyKeyBytes().toByteArrayKey() to
                      (overflowPage as OverflowPage).getValue(leafNode.valueSize)
                }
              }
            }
          }
}
