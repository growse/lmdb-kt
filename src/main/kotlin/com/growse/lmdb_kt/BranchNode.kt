package com.growse.lmdb_kt

import mu.KotlinLogging
import java.nio.ByteBuffer
import java.util.*

data class BranchNode(private val buffer: ByteBuffer) : Node {
	private val logger = KotlinLogging.logger {}
	val lo: UShort
	val hi: UShort
	val flags: BitSet
	val kSize: UShort
	val key: ByteArray
	val childPage: UInt

	init {
		logger.trace { "Parsing branch node at ${buffer.position()}" }
		lo = buffer.short.toUShort()
		hi = buffer.short.toUShort()
		flags = ByteArray(2).let {
			buffer.get(it)
			BitSet.valueOf(it)
		}
		kSize = buffer.short.toUShort()
		logger.trace { "Key is $kSize bytes" }
		logger.trace { "Reading key at ${buffer.position()}" }
		key = ByteArray(kSize.toInt()).apply(buffer::get)
		logger.trace { "Key value is ${key.toHex()} or ${key.toAscii()}" }
		childPage = lo + (hi.toUInt().shl(16))
		logger.trace { "Child page is at $childPage" }
	}
}
