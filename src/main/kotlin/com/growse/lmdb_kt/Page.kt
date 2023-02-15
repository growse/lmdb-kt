package com.growse.lmdb_kt

import mu.KotlinLogging
import java.util.*

private val logger = KotlinLogging.logger {}

interface Page {
	val buffer: DbMappedBuffer
	val number: UInt

	/**
	 * Dumps the keys/values from this page. Walks through to child pages. Copies data from the
	 * buffer.
	 *
	 * @return a map of keys/values
	 */
	fun dump(): Map<String, ByteArray>

	fun get(key: ByteArray): Result<ByteArray> {
		logger.trace { "Looking for ${key.toHex()} on page $number" }
		buffer.seek(number, 0u)
		return when (this) {
			is LeafPage -> {
				val leafNode = nodes.first { it.keyBytes().contentEquals(key) }
				logger.trace { "Found it in a leaf node: $leafNode" }
				Result.success(leafNode.valueBytes())
			}
			is BranchPage -> {
				nodes
					.last { it.keyBytes().compareWith(key) < 0 }
					.also {
						logger.trace { "Found it in a branch node. Going to child page: ${it.childPage}" }
					}
					.childPage
					.run(buffer::getPage)
					.get(key)
			}
			else -> {
				Result.failure(Exception("Root page is not a leaf or branch page"))
			}
		}
	}

	/** Page flags http://www.lmdb.tech/doc/group__mdb__page.html */
	enum class Flags(val _idx: Int) {
		BRANCH(0),
		LEAF(1),
		OVERFLOW(2),
		META(3),
		DIRTY(4),
		LEAF2(5),
		SUBP(6),
		LOOSE(14),
		KEEP(15),
	}

	class UnsupportedPageTypeException(private val flags: EnumSet<Flags>) : Throwable() {
		override fun toString(): String {
			return "${super.toString()} : $flags"
		}
	}
}
