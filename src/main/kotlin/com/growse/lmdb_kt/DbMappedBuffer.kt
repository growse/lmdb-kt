package com.growse.lmdb_kt

import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.ByteBuffer
import java.util.*

private val logger = KotlinLogging.logger {}

/**
 * A DbMappedBuffer is a mapped buffer that represents a [DB]. The [ByteBuffer] given should
 * represent the whole [DB] structure, and the given pagesize parameter lets it navigate and extract
 * different pages. It also implements some of the commonly used [ByteBuffer] methods, passing them
 * onto the underlying buffer.
 */
data class DbMappedBuffer(private val buffer: ByteBuffer, internal val pageSize: UInt) {

	/**
	 * Parses the given page number into the correct structure
	 *
	 * @param number the page number to parse
	 * @return the parsed page
	 */
	internal fun getPage(number: UInt): Page {
		logger.trace { "Getting page $number" }
		val pageStart = (number * this.pageSize).toInt()
		buffer.position(pageStart)
		val pageHeader = PageHeader(this, number)
		assert(
			pageHeader.storedPageNumber == number.toLong(),
		) {
			"Page number is not correct for page! Requested=$number, stored is ${pageHeader.storedPageNumber}"
		}
		return if (pageHeader.flags.contains(Page.Flags.META)) {
			buffer.position(pageStart + PageHeader.SIZE.toInt())
			MetaDataPage64(this, number)
		} else if (pageHeader.flags.contains(Page.Flags.LEAF)) {
			LeafPage(pageHeader, this, number)
		} else if (pageHeader.flags.contains(Page.Flags.OVERFLOW)) {
			OverflowPage(pageHeader, this, number)
		} else if (pageHeader.flags.contains(Page.Flags.BRANCH)) {
			BranchPage(pageHeader, this, number)
		} else {
			throw Page.UnsupportedPageTypeException(pageHeader.flags)
		}
	}

	/**
	 * Position the buffer cursor to a given page and offset within that page
	 *
	 * @param pageNumber
	 * @param offsetInPage
	 */
	fun seek(pageNumber: UInt, offsetInPage: UInt = 0u) {
		((pageNumber * pageSize) + offsetInPage).toInt().run {
			logger.trace { "Seek to page $pageNumber offset $offsetInPage (=$this)" }
			buffer.position(this)
		}
	}

	fun readInt(): Int {
		return buffer.int
	}

	fun readUInt(): UInt {
		return readInt().toUInt()
	}

	fun readShort(): Short {
		return buffer.short
	}

	fun readUShort(): UShort {
		return readShort().toUShort()
	}

	fun readLong(): Long {
		return buffer.long
	}

	fun readULong(): ULong {
		return readLong().toULong()
	}

	/**
	 * Slices a buffer out of the underlying buffer
	 *
	 * @param pageNumber The page to slice from
	 * @param index address on the page to slice
	 * @param length how much to slice
	 * @return a [ByteBuffer] representing the sliced data
	 */
	fun slice(pageNumber: UInt, index: Int, length: Int): ByteBuffer {
		val position = (pageNumber * pageSize).toInt() + index
		return buffer.run {
			position(position)
			slice().limit(length)
		}
	}

	/**
	 * Parses an [EnumSet] of type [T] out of the next [byteCount] bytes of the buffer.
	 *
	 * @param T Type of enum to extract
	 * @param clazz a class instance of [T]
	 * @param byteCount how many bytes should we parse
	 * @return a set of [T] containing the Enums for which the bits were set
	 */
	fun <T : Enum<T>> flags(clazz: Class<T>, byteCount: UShort): EnumSet<T> =
		BitSet.valueOf(buffer.slice().apply { limit(byteCount.toInt()) })
			.let { bitset ->
				buffer.seek(byteCount.toInt()) // Until everything is fully lazy, we need to advance the buffer
				EnumSet.allOf(clazz).apply { removeIf { flag -> !bitset[flag.ordinal] } }
			}

	/**
	 * Copies data out of the [buffer] into the given [ByteArray]
	 *
	 * @param bytes the [ByteArray] to copy data into
	 */
	fun copy(bytes: ByteArray) {
		buffer.get(bytes)
	}
}
