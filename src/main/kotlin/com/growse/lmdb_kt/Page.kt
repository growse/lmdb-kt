package com.growse.lmdb_kt

import mu.KotlinLogging
import java.util.*

private val logger = KotlinLogging.logger {}

interface Page {
	val buffer: ByteBufferWithPageSize
	val number: UInt
	val pageOffset: UInt
		get() = number * buffer.pageSize

	/**
	 * Dumps the keys/values from a specific page. Walks through to child pages
	 *
	 * @param page the page to dump data for
	 * @return a map of keys/values
	 */
	fun dump(): Map<String, ByteArray> {
		buffer.buffer.position((number * buffer.pageSize).toInt())
		when (this) {
			is LeafPage -> {
				return nodes.associate { leafNode ->
					when (leafNode.value) {
						is Either.Left -> {
							String(leafNode.key) to (leafNode.value as Either.Left<ByteArray, Long>).left
						}

						// It's an overflow value
						is Either.Right -> {
							val overflowPage = buffer.getPage((leafNode.value as Either.Right<ByteArray, Long>).right.toUInt())
							assert(overflowPage is OverflowPage)
							String(leafNode.key) to (overflowPage as OverflowPage).getValue(leafNode.valueSize)
						}
					}
				}
			}

			is BranchPage -> {
				return nodes.map { nodeAddress ->
					logger.trace { "Branch node points to page at ${nodeAddress.childPage}" }
					buffer.getPage(nodeAddress.childPage).dump()
				}.fold(mutableMapOf()) { acc, map -> acc.apply { putAll(map) } }
			}

			else -> TODO("Not implemented")
		}
	}

	fun get(key: String): Result<ByteArray> {
		logger.trace { "Looking for $key on page $number" }
		buffer.buffer.position((number * buffer.pageSize).toInt())
		return when (this) {
			is LeafPage -> {
				val leafNode = nodes.first { it.key.contentEquals(key.toByteArray()) }
				logger.trace { "Found it in a leaf node: $leafNode" }
				when (leafNode.value) {
					is Either.Left -> Result.success((leafNode.value as Either.Left<ByteArray, Long>).left)
					is Either.Right -> {
						val overflowPage = buffer.getPage((leafNode.value as Either.Right<ByteArray, Long>).right.toUInt())
						assert(overflowPage is OverflowPage)
						Result.success((overflowPage as OverflowPage).getValue(leafNode.valueSize))
					}
				}
			}

			is BranchPage -> {
				nodes
					.last { it.key.compareWith(key.toByteArray()) < 0 }
					.also { logger.trace { "Found it in a branch node. Going to child page: ${it.childPage}" } }.childPage
					.run(buffer::getPage)
					.get(key)
			}

			else -> {
				Result.failure(Exception("Root page is not a leaf or branch page"))
			}
		}
	}

	/**
	 * Page flags
	 * http://www.lmdb.tech/doc/group__mdb__page.html
	 */
	enum class Flags(val _idx: Int) {
		BRANCH(0),
		LEAF(1),
		OVERFLOW(2),
		META(3),
		DIRTY(4),
		LEAF2(5),
		SUBP(6),
		LOOSE(14),
		KEEP(15)
	}

	class UnsupportedPageTypeException(private val flags: EnumSet<Flags>) : Throwable() {
		override fun toString(): String {
			return "${super.toString()} : $flags"
		}
	}
}
