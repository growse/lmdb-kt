package com.growse.lmdb_kt

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

data class BranchPage(
    override val buffer: DbMappedBuffer,
    override val number: UInt,
    val pageHeader: PageHeader,
) : Page {
  constructor(
      pageHeader: PageHeader,
      buffer: DbMappedBuffer,
      number: UInt,
  ) : this(buffer = buffer, number = number, pageHeader)

  private val nodes: List<BranchNode>
    get() {
      buffer.seek(number, PageHeader.SIZE)
      logger.trace { "Branch page has ${pageHeader.numKeys()} keys" }
      return IntRange(1, pageHeader.numKeys())
          .map { buffer.readShort().also { logger.trace { "key at $it" } } }
          .map { BranchNode(buffer, number, it.toUInt()) }
    }

  override fun get(key: ByteArray): Result<ByteArray> =
      nodes
          .also {
            logger.trace { "Key get. Looking for ${key.toPrintableString()} on page $number" }
          }
          .last { it.copyKeyBytes().compareWith(key) < 0 }
          .also {
            logger.trace { "Found it in a branch node. Going to child page: ${it.childPage}" }
          }
          .childPage
          .run(buffer::getPage)
          .get(key)

  override fun dump(): Map<ByteArrayKey, ByteArray> =
      logger
          .trace { "Dump Branchpage ${this.number}" }
          .run {
            nodes
                .map { nodeAddress ->
                  logger.trace { "Branch node points to page at ${nodeAddress.childPage}" }
                  buffer.getPage(nodeAddress.childPage).dump()
                }
                .fold(mutableMapOf()) { acc, map -> acc.apply { putAll(map) } }
          }
}
