package com.growse.lmdb_kt

import mu.KotlinLogging
import java.nio.ByteBuffer
import java.util.*

private val logger = KotlinLogging.logger {}

/**
 * A structure that represents a single key/value pair. Bundled together to form a [LeafPage]. Each node can either
 * contain the key and value contiguously if they fit within the space available in the page, or with the [NODE_BIGDATA]
 * flag set the node will point towards another [OverflowPage] which contains the value.
 *
 * @constructor
 * Parses the node data from the buffer
 *
 * @param buffer a [ByteBuffer] that contains the Node
 */
data class LeafNode(
	private val buffer: ByteBuffer
) : Node {
	private val position = buffer.position()
	private val lo: UShort = buffer.short.toUShort()
	private val hi: UShort = buffer.short.toUShort()
	private val flags: EnumSet<Node.Flags> = flagsFromBuffer(Node.Flags::class.java, buffer, 2u)
	private val keySize: UShort = buffer.short.toUShort()
	val key: ByteArray = ByteArray(keySize.toInt()).apply(buffer::get)
	val valueSize: UInt = lo + (hi.toUInt().shl(16))

	val value: Either<ByteArray, Long> // The value is either a bytearray or a reference to an overflow page
		by lazy {
			buffer.position(position + 8 + keySize.toInt())
			if (flags.contains(Node.Flags.BIGDATA)) {
				Either.Right(buffer.long.also { logger.trace { "Value is bigdata at page $it" } })
			} else {
				Either.Left(
					ByteArray(valueSize.toInt())
						.apply(buffer::get)
						.also { logger.trace { "Value is $valueSize bytes (${it.toHex()} or ${it.toAscii()})" } }
				)
			}
		}

	override fun toString(): String {
		return "LeafNode(position=$position)"
	}
}
