package com.growse.lmdb_kt

import java.nio.Buffer


internal fun Buffer.positionRelative(offset: Int) {
    this.position(this.position() + offset)
}

internal fun ByteArray.toHex(): String = joinToString(separator = "") { eachByte -> "%02x".format(eachByte) }
internal fun ByteArray.toAscii(): String = String(this)
