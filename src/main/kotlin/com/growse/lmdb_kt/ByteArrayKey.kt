package com.growse.lmdb_kt

data class ByteArrayKey(val bytes: ByteArray) {
  val size = bytes.size

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as ByteArrayKey

    if (!bytes.contentEquals(other.bytes)) return false

    return true
  }

  override fun hashCode(): Int {
    return bytes.contentHashCode()
  }
}

fun ByteArray.toByteArrayKey() = ByteArrayKey(this)

fun String.toByteArrayKey() = ByteArrayKey(this.toByteArray())
