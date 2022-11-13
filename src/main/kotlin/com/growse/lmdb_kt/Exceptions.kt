package com.growse.lmdb_kt

import java.util.*

class UnableToDetectPageSizeException : Throwable()

class UnsupportedPageTypeException(private val flags: EnumSet<Flags.Page>) : Throwable() {
	override fun toString(): String {
		return "${super.toString()} : $flags"
	}
}
