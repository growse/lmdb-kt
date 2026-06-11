package com.growse.lmdb_kt

import java.nio.Buffer
import java.nio.ByteBuffer

internal fun Buffer.seek(offset: Int) {
  this.position(this.position() + offset)
}

internal fun ByteBuffer.copyRemainingBytes(): ByteArray =
    duplicate().run {
      ByteArray(remaining()).also(::get)
    }

internal fun ByteBuffer.readOnlySlice(): ByteBuffer = duplicate().slice().asReadOnlyBuffer()

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
    firstDifference != null ->
        if (firstDifference.first.toUByte() > firstDifference.second.toUByte()) 1 else -1
    this.size > byteArray.size -> 1
    else -> -1
  }
}

internal fun ByteBuffer.contentEquals(byteArray: ByteArray): Boolean {
  if (remaining() != byteArray.size) {
    return false
  }
  val duplicate = duplicate()
  byteArray.forEach { expected ->
    if (duplicate.get() != expected) {
      return false
    }
  }
  return true
}

internal fun ByteBuffer.compareWith(byteArray: ByteArray): Int {
  val duplicate = duplicate()
  val length = duplicate.remaining()
  val commonLength = minOf(length, byteArray.size)
  repeat(commonLength) { index ->
    val bufferByte = duplicate.get().toUByte()
    val inputByte = byteArray[index].toUByte()
    if (bufferByte != inputByte) {
      return if (bufferByte > inputByte) 1 else -1
    }
  }
  return when {
    length > byteArray.size -> 1
    length < byteArray.size -> -1
    else -> 0
  }
}
