package com.growse.lmdb_kt

import mu.KotlinLogging
import java.nio.ByteBuffer
import java.util.*

private val logger = KotlinLogging.logger {}

data class BranchNode(
	private val buffer: DbMappedBuffer,
	private val pageNumber: UInt,
	private val addressInPage: UInt
) : Node(buffer, pageNumber, addressInPage) {
	val childPage: UInt = lo + (hi.toUInt().shl(16))
}
