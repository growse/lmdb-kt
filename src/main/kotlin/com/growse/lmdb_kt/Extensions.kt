package com.growse.lmdb_kt

import java.nio.Buffer
import java.nio.ByteBuffer
import java.util.*

internal fun Buffer.seek(offset: Int) {
	this.position(this.position() + offset)
}

internal fun ByteArray.toHex(): String = joinToString(separator = "") { eachByte -> "%02x".format(eachByte) }
internal fun ByteArray.toAscii(): String = String(this)

internal fun ByteArray.compareWith(byteArray: ByteArray): Int {
	if (this.contentEquals(byteArray)) {
		return 0
	}
	if (this.isEmpty()) return -1
	if (byteArray.isEmpty()) return 1
	return if (this.zip(byteArray).first { it.first != it.second }.run { first > second }) 1 else -1
}

internal fun <T : Enum<T>> flagsFromBuffer(clazz: Class<T>, buffer: ByteBuffer, byteCount: UShort): EnumSet<T> =
	BitSet.valueOf(buffer.slice(buffer.position(), byteCount.toInt())).let { bitset ->
		buffer.seek(byteCount.toInt())
		val constants = clazz.enumConstants
		EnumSet.noneOf(clazz).apply {
			addAll(
				IntRange(0, bitset.size()).flatMap {
					if (bitset[it]) listOf(constants[it]) else emptyList()
				}
			)
		}
	}
