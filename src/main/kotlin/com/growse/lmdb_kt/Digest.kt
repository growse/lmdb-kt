package com.growse.lmdb_kt

import java.security.MessageDigest

internal fun ByteArray.digest(): String = MessageDigest.getInstance("MD5").digest(this).toHex()
