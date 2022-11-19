package com.growse.lmdb_kt

import com.growse.lmdb_kt.Environment.Stat
import mu.KotlinLogging
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.util.*
import kotlin.io.path.fileSize
import kotlin.io.path.isDirectory
import kotlin.io.path.isRegularFile

/**
 * LMDB Environment. Represents a directory on disk containing one or more databases.
 *
 * @constructor
 * Detects the page size of the database and extract the metadata page, which can be used to populate the [Stat] structure
 *
 * @param lmdbPath path to the LMDB directory on disk
 * @param pageSize optionally specify the page size to use. If not specified, auto-detection is attempted
 */
class Environment(
	lmdbPath: Path,
	pageSize: UInt? = null,
	readOnly: Boolean = false,
	locking: Boolean = true,
	byteOrder: ByteOrder = ByteOrder.nativeOrder()
) : AutoCloseable {
	private val logger = KotlinLogging.logger {}
	private val fileChannel: FileChannel
	private val byteBufferWithPageSize: ByteBufferWithPageSize
	private val supportedPageSizes = listOf(4u * 1024u, 8u * 1024u, 16u * 1024u)

	private val metadata: MetaDataPage64

	val stat: Stat

	/**
	 * Constructor will detect the page size of the database and extract the metadata page, which can be used to populate
	 * the [Stat] structure
	 */
	init {
		assert(readOnly) { "Writes are not currently implemented" }
		assert(!locking) { "Locking is not currently implemented" }
		assert(lmdbPath.isDirectory()) { "Supplied path is not a directory" }

		val dataFile = lmdbPath.resolve(DATA_FILENAME)
		val lockFile = lmdbPath.resolve(LOCK_FILENAME)
		assert(dataFile.isRegularFile()) { "Supplied path does not contain a data file" }
		if (locking) {
			assert(lockFile.isRegularFile()) { "Supplied path does not contain a lock file" }
		}

		logger.trace { "Mapping file $dataFile" }
		fileChannel = FileChannel.open(dataFile)
		val mappedFile =
			fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataFile.fileSize()).apply { order(byteOrder) }

		val (metadata, detectedPageSize) = if (pageSize != null) {
			getMetadataPage(mappedFile, listOf(pageSize))
		} else {
			getMetadataPage(mappedFile)
		}

		assert(dataFile.fileSize() % detectedPageSize.toInt() == 0L) { "Data file is not a multiple of the detected page size of $pageSize" }
		// TODO work out what the right lock file size should be...? It's 8KB on a 16KB pagesize db
//        assert(lockFile.fileSize() % pageSize.toInt() == 0L) { "Lock file is not a valid size" }

		byteBufferWithPageSize = ByteBufferWithPageSize(mappedFile, detectedPageSize)

		this.metadata = metadata
		this.stat = Stat(
			detectedPageSize,
			metadata.mainDb.depth.toUInt(),
			metadata.mainDb.branchPages,
			metadata.mainDb.leafPages,
			metadata.mainDb.overflowPages,
			metadata.mainDb.entries
		)

		val rootPageNumber = metadata.mainDb.rootPageNumber
		if (rootPageNumber >= 0) { // -1 is an empty db
			assert(rootPageNumber <= UInt.MAX_VALUE.toLong())
		}
	}

	fun getMainDb(): DB {
		return metadata.mainDb
	}

	override fun close() {
		logger.trace { "Close" }
		if (fileChannel.isOpen) {
			fileChannel.close()
		}
	}

	/**
	 * struct MDB_stat
	 * Statistics for a database in the environment.
	 * unsigned int 	ms_psize
	 * unsigned int 	ms_depth
	 * size_t 	ms_branch_pages
	 * size_t 	ms_leaf_pages
	 * size_t 	ms_overflow_pages
	 * size_t 	ms_entries
	 */
	data class Stat(
		val pageSize: UInt,
		val treeDepth: UInt,
		val branchPagesCount: Long,
		val leafPagesCount: Long,
		val overflowPagesCount: Long,
		val entriesCount: Long
	)

	/**
	 * Gets the metadata page. Also tries to detect and return the current page size (because that's a function of whatever
	 * _SC_PAGESIZE happens to be on the system generating the database. Often it's 4KB, sometimes 16KB)
	 * @param testPageSizes a list of page sizes to try
	 * @return the parsed metadatapage and the detected pageSize
	 */
	private fun getMetadataPage(
		buffer: ByteBuffer,
		testPageSizes: Collection<UInt> = supportedPageSizes
	): Pair<MetaDataPage64, UInt> {
		testPageSizes.forEach { testPageSize ->
			try {
				val page = getMetadataPageWithPageSize(buffer, testPageSize)
				return Pair(page, testPageSize)
			} catch (e: Throwable) {
				when (e) {
					is AssertionError, is Page.UnsupportedPageTypeException -> {
						logger.debug { "Page size is not $testPageSize" }
					}

					else -> throw e
				}
			}
		}
		throw UnableToDetectPageSizeException()
	}

	private fun getMetadataPageWithPageSize(buffer: ByteBuffer, pageSize: UInt): MetaDataPage64 {
		val first = ByteBufferWithPageSize(buffer, pageSize).getPage(0u)
		assert(first is MetaDataPage64) { "First page is not a metadata page" }
		val second = ByteBufferWithPageSize(buffer, pageSize).getPage(1u)
		assert(second is MetaDataPage64) { "Second page is not a metadata page" }
		val latestMetadataPage = if ((first as MetaDataPage64).txnId > (second as MetaDataPage64).txnId) first else second
		assert(latestMetadataPage.magic == 0xBEEFC0DE.toUInt()) { "Page does not contain required magic. Instead ${latestMetadataPage.magic}" }
		assert(latestMetadataPage.version == 1u) { "Invalid page version ${latestMetadataPage.version}" } // Not supporting development version 999
		logger.trace { "Page size is $pageSize" }
		return latestMetadataPage
	}

	/**
	 * A structure representing a byte range in a buffer
	 *
	 * @property lower
	 * @property upper
	 */
	data class Range(
		val lower: UShort,
		val upper: UShort
	)

	companion object {
		private const val DATA_FILENAME = "data.mdb"
		private const val LOCK_FILENAME = "lock.mdb"
	}
}
