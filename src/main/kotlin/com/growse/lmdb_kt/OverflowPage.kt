package com.growse.lmdb_kt

data class OverflowPage(
	override val buffer: ByteBufferWithPageSize,
	override val number: UInt,
	val pageHeader: PageHeader
) : Page {
	constructor(pageHeader: PageHeader, byteBufferWithPageSize: ByteBufferWithPageSize, number: UInt) : this(
		buffer = byteBufferWithPageSize,
		number = number,
		pageHeader
	)

	/**
	 * Gets the value of an overflow page. Overflow values start at an overflow page, and then run through contiguous
	 * pages
	 *
	 * @param valueSize the size of the overflow value to read. Should be less thon number of overflow pages * pageSize
	 * @return the value from reading the page (and any subsequent)
	 */
	fun getValue(valueSize: UInt): ByteArray {
		assert(pageHeader.pagesOrRange is Either.Left) { "Overflow page does not have pageCount value" }
		val numberOfPages = (pageHeader.pagesOrRange as Either.Left).left
		assert(valueSize < numberOfPages * buffer.pageSize) { "Requested value size is bigger than fits in number of pages" }
		return ByteArray(valueSize.toInt()).apply {
			buffer.buffer.position(((pageHeader.pageNumber * buffer.pageSize.toInt()) + PageHeader.SIZE.toInt()).toInt())
			buffer.buffer.get(this)
		}
	}
}
