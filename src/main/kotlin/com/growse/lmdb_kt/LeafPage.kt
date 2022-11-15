package com.growse.lmdb_kt

import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * A Leaf page contains a bunch of [LeafNode]s.
 *
 * @property pageHeader the page header
 * @property nodes a list of [LeafNode]
 */
data class LeafPage(
	override val buffer: ByteBufferWithPageSize,
	override val number: UInt,
	val pageHeader: PageHeader
) : Page {
	constructor(
		pageHeader: PageHeader,
		byteBufferWithPageSize: ByteBufferWithPageSize,
		number: UInt
	) : this(
		buffer = byteBufferWithPageSize,
		number = number,
		pageHeader
	)

	val nodes = IntRange(1, pageHeader.numKeys())
		.also { logger.trace { "Leaf page has ${pageHeader.numKeys()} keys" } }
		.map { buffer.buffer.short }
		.map {
			buffer.buffer.position(pageOffset.toInt() + it)
			LeafNode(buffer.buffer)
		}
}
