package com.growse.lmdb_kt

import java.nio.Buffer

internal fun Buffer.seek(offset: Int) {
  this.position(this.position() + offset)
}

@OptIn(ExperimentalStdlibApi::class)
internal fun ByteArray.toPrintableString(): String =
    if (this.all { it in 0x20..0x7e }) {
      "'${String(this)}' (0x${this.toHexString()})"
    } else {
      "(0x${this.toHexString()})"
    }

internal fun ByteArray.compareWith(byteArray: ByteArray): Int {
  if (this.contentEquals(byteArray)) {
    return 0
  }
  if (this.isEmpty()) return -1
  if (byteArray.isEmpty()) return 1
  val firstDifference = this.zip(byteArray).firstOrNull { it.first != it.second }
  return when {
    firstDifference != null -> if (firstDifference.first > firstDifference.second) 1 else -1
    this.size > byteArray.size -> 1
    else -> -1
  }
}
