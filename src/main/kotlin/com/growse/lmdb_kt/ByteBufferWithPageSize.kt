package com.growse.lmdb_kt

import mu.KotlinLogging
import java.nio.ByteBuffer

data class ByteBufferWithPageSize(val buffer: ByteBuffer, val pageSize: UInt) {
	private val logger = KotlinLogging.logger {}

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
		val pageHeader = PageHeader(buffer)
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
}
