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
			return IntRange(1, pageHeader.numKeys())
				.also { logger.trace { "Branch page has ${pageHeader.numKeys()} keys" } }
				.map { buffer.readShort().also { logger.trace { "key at $it" } } }
				.map { BranchNode(buffer, number, it.toUInt()) }
		}
}
