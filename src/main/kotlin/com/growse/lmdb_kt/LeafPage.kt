package com.growse.lmdb_kt

import mu.KotlinLogging
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

	val nodes: List<LeafNode>
		get() {
			buffer.seek(number, PageHeader.SIZE)
			logger.trace { "Leaf page has ${pageHeader.numKeys()} keys" }
			return IntRange(1, pageHeader.numKeys())
				.map { buffer.readShort().also { logger.trace { "key at $it" } } }
				.map { LeafNode(buffer, number, it.toUInt()) }
		}

	override fun dump(): Map<String, ByteArray> =
		nodes.associate { leafNode ->
			when (leafNode.value) {
				// It's an in-line value
				is Either.Left -> {
					String(ByteArray(leafNode.key.capacity()).apply(leafNode.key::get)) to
						ByteArray(leafNode.valueSize.toInt())
							.apply((leafNode.value as Either.Left<ByteBuffer, *>).left::get)
				}

				// It's an overflow value
				is Either.Right -> {
					val overflowPage =
						buffer.getPage((leafNode.value as Either.Right<ByteBuffer, Long>).right.toUInt())
					assert(overflowPage is OverflowPage)
					String(leafNode.keyBytes()) to
						(overflowPage as OverflowPage).getValue(leafNode.valueSize)
				}
			}
		}
}
