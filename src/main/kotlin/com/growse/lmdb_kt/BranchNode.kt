package com.growse.lmdb_kt

import mu.KotlinLogging
import java.nio.ByteBuffer
import java.util.*

private val logger = KotlinLogging.logger {}

data class BranchNode(private val buffer: ByteBuffer) : Node {
	private val lo: UShort = buffer.short.toUShort()
	private val hi: UShort = buffer.short.toUShort()
	val childPage: UInt = lo + (hi.toUInt().shl(16))
	private val flags: BitSet = ByteArray(2).let {
		buffer.get(it)
		BitSet.valueOf(it)
	}
	private val keySize: UShort = buffer.short.toUShort()
	val key: ByteArray = ByteArray(keySize.toInt())
		.apply(buffer::get)
		.also { logger.trace { "Key is $keySize bytes (${it.toHex().take(12)}...), child page at $childPage" } }
}
