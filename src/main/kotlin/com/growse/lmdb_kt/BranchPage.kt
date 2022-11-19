package com.growse.lmdb_kt

import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

data class BranchPage(
	override val buffer: ByteBufferWithPageSize,
	override val number: UInt,
	val pageHeader: PageHeader
) : Page {
	constructor(pageHeader: PageHeader, buffer: ByteBufferWithPageSize, number: UInt) : this(
		buffer = buffer,
		number = number,
		pageHeader
	)

	val nodes: List<BranchNode>
		get() {
			buffer.buffer.position(((buffer.pageSize * number)+PageHeader.SIZE).toInt())
			return IntRange(1, pageHeader.numKeys())
				.also { logger.trace { "Branch page has ${pageHeader.numKeys()} keys" } }
				.map { buffer.buffer.short }
				.map {
					buffer.buffer.position(pageOffset.toInt() + it)
					BranchNode(buffer.buffer)
				}
		}
}
