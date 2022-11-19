package com.growse.lmdb_kt

import mu.KotlinLogging
import java.nio.ByteBuffer
import java.util.*

private val logger = KotlinLogging.logger {}

data class BranchNode(private val buffer: ByteBuffer) : Node {
	val lo: UShort
	val hi: UShort
	val flags: BitSet
	val kSize: UShort
	val key: ByteArray
	val childPage: UInt

	init {
		lo = buffer.short.toUShort()
		hi = buffer.short.toUShort()
		flags = ByteArray(2).let {
			buffer.get(it)
			BitSet.valueOf(it)
		}
		kSize = buffer.short.toUShort()
		childPage = lo + (hi.toUInt().shl(16))
		key = ByteArray(kSize.toInt()).apply(buffer::get)
		logger.trace { "Key is $kSize bytes (${key.toHex().take(12)}...), child page at $childPage" }
	}
}
