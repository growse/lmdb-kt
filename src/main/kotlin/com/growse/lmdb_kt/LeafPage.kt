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
	override val buffer: DbMappedBuffer,
	override val number: UInt,
	val pageHeader: PageHeader,
) : Page {
	constructor(
		pageHeader: PageHeader,
		dbMappedBuffer: DbMappedBuffer,
		number: UInt,
	) : this(buffer = dbMappedBuffer, number = number, pageHeader)

	val nodes: List<LeafNode>
		get() {
			buffer.seek(number, PageHeader.SIZE)
			return IntRange(1, pageHeader.numKeys())
				.also { logger.trace { "Leaf page has ${pageHeader.numKeys()} keys" } }
				.map { buffer.readShort().also { logger.trace { "key at $it" } } }
				.map { LeafNode(buffer, number, it.toUInt()) }
		}
}
