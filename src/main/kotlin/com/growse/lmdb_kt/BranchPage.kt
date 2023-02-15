package com.growse.lmdb_kt

import mu.KotlinLogging

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

	val nodes: List<BranchNode>
		get() {
			buffer.seek(number, PageHeader.SIZE)
			logger.trace { "Branch page has ${pageHeader.numKeys()} keys" }
			return IntRange(1, pageHeader.numKeys())
				.map { buffer.readShort().also { logger.trace { "key at $it" } } }
				.map { BranchNode(buffer, number, it.toUInt()) }
		}

	override fun dump(): Map<String, ByteArray> =
		nodes
			.map { nodeAddress ->
				logger.trace { "Branch node points to page at ${nodeAddress.childPage}" }
				buffer.getPage(nodeAddress.childPage).dump()
			}
			.fold(mutableMapOf()) { acc, map -> acc.apply { putAll(map) } }
}
