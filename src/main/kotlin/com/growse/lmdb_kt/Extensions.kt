package com.growse.lmdb_kt

import java.nio.Buffer

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
