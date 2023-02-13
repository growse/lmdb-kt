package com.growse.lmdb_kt

data class BranchNode(
	private val buffer: DbMappedBuffer,
	private val pageNumber: UInt,
	private val addressInPage: UInt,
) : Node(buffer, pageNumber, addressInPage) {
	val childPage: UInt = lo + (hi.toUInt().shl(16))
}
