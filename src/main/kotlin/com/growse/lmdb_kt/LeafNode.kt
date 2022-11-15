package com.growse.lmdb_kt

import mu.KotlinLogging
import java.nio.ByteBuffer
import java.util.*

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
	private val logger = KotlinLogging.logger {}
	val lo: UShort
	val hi: UShort
	val flags: EnumSet<Node.Flags>
	val kSize: UShort
	val valueSize: UInt
	val key: ByteArray

	val value: Either<ByteArray, Long> // The value is either a bytearray or a reference to an overflow page

	init {
		logger.trace { "Parsing leaf node at ${buffer.position()}" }
		lo = buffer.short.toUShort()
		hi = buffer.short.toUShort()
		flags = flagsFromBuffer(Node.Flags::class.java, buffer, 2u)
		kSize = buffer.short.toUShort()
		logger.trace { "Key is $kSize bytes" }
		logger.trace { "Reading key at ${buffer.position()}" }
		key = ByteArray(kSize.toInt()).apply(buffer::get)
		logger.trace { "Key value is ${key.toHex()} or ${key.toAscii()}" }
		valueSize = lo + (hi.toUInt().shl(16))

		value = if (flags.contains(Node.Flags.BIGDATA)) {
			Either.Right(buffer.long.also { logger.trace { "Value is bigdata at page $it" } })
		} else {
			logger.trace { "Value is $valueSize bytes at ${buffer.position()}" }
			Either.Left(
				ByteArray(valueSize.toInt())
					.apply(buffer::get)
					.also { logger.trace { "Value is ${it.toHex()} or ${it.toAscii()}" } }
			)
		}
	}
}
