package com.growse.lmdb_kt

class Transaction(val environment: Environment, val write: Boolean = false) {
	init {
		assert(!write) { "Database writes not yet supported " }
	}
}
