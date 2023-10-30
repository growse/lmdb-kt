package com.growse.lmdb_kt

import java.security.MessageDigest

@OptIn(ExperimentalStdlibApi::class)
internal fun ByteArray.digest(): String =
    MessageDigest.getInstance("MD5").digest(this).toHexString()
