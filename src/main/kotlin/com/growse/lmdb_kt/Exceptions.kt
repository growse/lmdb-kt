package com.growse.lmdb_kt

import java.util.*

class UnableToDetectPageSizeException : Throwable()

class UnsupportedPageTypeException(private val flags: BitSet) : Throwable() {
    override fun toString(): String {
        return "${super.toString()} : $flags"
    }
}