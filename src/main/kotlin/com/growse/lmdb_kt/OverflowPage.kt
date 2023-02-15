package com.growse.lmdb_kt

data class OverflowPage(
	override val buffer: DbMappedBuffer,
	override val number: UInt,
	val pageHeader: PageHeader,
) : Page {
	constructor(
		pageHeader: PageHeader,
		dbMappedBuffer: DbMappedBuffer,
		number: UInt,
	) : this(buffer = dbMappedBuffer, number = number, pageHeader)

	/**
	 * Gets the value of an overflow page. Overflow values start at an overflow page, and then run
	 * through contiguous pages.
	 *
	 * Copies data from the underlying buffer
	 *
	 * @param valueSize the size of the overflow value to read. Should be less thon number of overflow
	 *   pages * pageSize
	 * @return the value from reading the page (and any subsequent)
	 */
	fun getValue(valueSize: UInt): ByteArray {
		assert(pageHeader.pagesOrRange is Either.Left) { "Overflow page does not have pageCount value" }
		val numberOfPages = (pageHeader.pagesOrRange as Either.Left).left
		assert(valueSize < numberOfPages * buffer.pageSize) {
			"Requested value size is bigger than fits in number of pages"
		}
		return ByteArray(valueSize.toInt()).also { array ->
			buffer.run {
				seek(number, PageHeader.SIZE)
				copy(array)
			}
		}
	}

	override fun dump(): Map<String, ByteArray> {
		throw AssertionError("Can't dump an overflow page directly")
	}

	override fun get(key: ByteArray): Result<ByteArray> = Result.failure(
		Exception("Can't get a value from an overflow page"),
	)
}
