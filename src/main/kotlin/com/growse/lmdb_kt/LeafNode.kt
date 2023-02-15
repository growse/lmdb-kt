package com.growse.lmdb_kt

import mu.KotlinLogging
import java.nio.ByteBuffer
import java.util.*

private val logger = KotlinLogging.logger {}

/**
 * A structure that represents a single key/value pair. Bundled together to form a [LeafPage]. Each
 * node can either contain the key and value contiguously if they fit within the space available in
 * the page, or with the [NODE_BIGDATA] flag set the node will point towards another [OverflowPage]
 * which contains the value.
 *
 * @param buffer a [ByteBuffer] that contains the Node
 * @constructor Parses the node data from the buffer
 */
data class LeafNode(
	private val buffer: DbMappedBuffer,
	private val pageNumber: UInt,
	private val addressInPage: UInt,
) : Node(buffer, pageNumber, addressInPage) {
	private val valueIndex = 8u + keySize
	val valueSize: UInt = lo + (hi.toUInt().shl(16))

	val value:
		Either<ByteBuffer, Long> // The value is either a bytearray or a reference to an overflow page
		by lazy {
			if (flags.contains(Flags.BIGDATA)) {
				buffer.seek(pageNumber, valueIndex + addressInPage)
				Either.Right(buffer.readLong().also { logger.trace { "Value is bigdata at page $it" } })
			} else {
				buffer.seek(pageNumber, valueIndex + addressInPage)
				Either.Left(
					buffer.slice(pageNumber, addressInPage.toInt() + 8 + keySize.toInt(), valueSize.toInt()),
				)
			}
		}

	fun valueBytes(): ByteArray {
		return when (value) {
			is Either.Left ->
				ByteArray(valueSize.toInt()).apply {
					buffer.seek(pageNumber, addressInPage)
					(value as Either.Left<ByteBuffer, Long>).left.get(this)
				}
			is Either.Right ->
				(
					buffer.getPage((value as Either.Right<ByteBuffer, Long>).right.toUInt())
						.also {
							assert(it is OverflowPage)
						} as OverflowPage
					)
					.getValue(valueSize)
		}
	}

	override fun toString(): String {
		return "LeafNode(page=$pageNumber, addressInPage=$addressInPage)"
	}
}
