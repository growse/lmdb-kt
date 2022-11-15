package com.growse.lmdb_kt

interface Node {
	/**
	 * Node flags
	 * http://www.lmdb.tech/doc/group__mdb__node.html
	 */
	enum class Flags(val _idx: Int) {
		BIGDATA(0),
		SUBDATA(1),
		DUPDATA(2)
	}
}
