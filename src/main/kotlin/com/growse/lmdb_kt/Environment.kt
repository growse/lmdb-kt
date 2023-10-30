package com.growse.lmdb_kt

import com.growse.lmdb_kt.Environment.Stat
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.fileSize
import kotlin.io.path.isDirectory
import kotlin.io.path.isRegularFile
import kotlin.io.path.readBytes

private val logger = KotlinLogging.logger {}

/**
 * LMDB Environment. Represents a directory on disk containing one or more databases.
 *
 * @param lmdbPath path to the LMDB directory on disk
 * @param pageSize optionally specify the page size to use. If not specified, auto-detection is
 *   attempted
 * @constructor Detects the page size of the database and extract the metadata page, which can be
 *   used to populate the [Stat] structure
 */
class Environment(
    lmdbPath: String,
    pageSize: UInt? = null,
    readOnly: Boolean = false,
    locking: Boolean = true,
    private val byteOrder: ByteOrder = ByteOrder.nativeOrder(),
) : AutoCloseable {
  constructor(
      lmdbPath: Path,
      pageSize: UInt? = null,
      readOnly: Boolean = false,
      locking: Boolean = true,
      byteOrder: ByteOrder = ByteOrder.nativeOrder(),
  ) : this(lmdbPath.toString(), pageSize, readOnly, locking, byteOrder)

  private var dataFile: Path
  private var lockFile: Path
  private var dataFileChannel: FileChannel

  private val mainDbMappedDbMappedBuffer: DbMappedBuffer
  private val supportedPageSizes = listOf(4u * 1024u, 8u * 1024u, 16u * 1024u)

  private var metadataPages: Pair<MetaDataPage64, MetaDataPage64>

  /**
   * Constructor will detect the page size of the database and extract the metadata page, which can
   * be used to populate the [Stat] structure
   */
  init {
    val dirPath = Paths.get(lmdbPath)
    assert(readOnly) { "Writes are not currently implemented" }
    assert(!locking) { "Locking is not currently implemented" }
    assert(dirPath.isDirectory()) { "Supplied path is not a directory" }

    dataFile = dirPath.resolve(DATA_FILENAME)
    lockFile = dirPath.resolve(LOCK_FILENAME)
    assert(dataFile.isRegularFile()) { "Supplied path does not contain a data file" }
    if (locking) {
      assert(lockFile.isRegularFile()) { "Supplied path does not contain a lock file" }
    }

    logger.trace {
      "Mapping file $dataFile. Size is ${dataFile.fileSize()}, md5 is ${dataFile.readBytes().digest()}"
    }
    dataFileChannel = FileChannel.open(dataFile)

    val mappedFile =
        dataFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataFile.fileSize()).apply {
          order(byteOrder)
        }

    val (metadataPointers, detectedPageSize) =
        if (pageSize != null) {
          getMetadataPage(mappedFile, listOf(pageSize))
        } else {
          getMetadataPage(mappedFile)
        }

    assert(dataFile.fileSize() % detectedPageSize.toInt() == 0L) {
      "Data file is not a multiple of the detected page size of $pageSize"
    }

    metadataPages = metadataPointers
    logger.trace { "Setting up mapped buffer on mainDb" }
    mainDbMappedDbMappedBuffer = DbMappedBuffer(mappedFile, detectedPageSize)
  }

  fun latestMetadataPage(): MetaDataPage64 {
    return if (metadataPages.first.txnId > metadataPages.second.txnId) {
      metadataPages.first
    } else {
      metadataPages.second
    }
  }

  fun stat(): Stat {
    logger.trace { "Fetching database stats" }
    return latestMetadataPage().run {
      Stat(
          mainDbMappedDbMappedBuffer.pageSize,
          mainDb.depth.toUInt(),
          mainDb.branchPages,
          mainDb.leafPages,
          mainDb.overflowPages,
          mainDb.entries,
      )
    }
  }

  fun beginTransaction(): Transaction {
    return Transaction(this)
  }

  override fun close() {
    logger.trace { "Close" }
    if (dataFileChannel.isOpen) {
      dataFileChannel.close()
    }
  }

  /**
   * struct MDB_stat Statistics for a database in the environment. unsigned int ms_psize unsigned
   * int ms_depth size_t ms_branch_pages size_t ms_leaf_pages size_t ms_overflow_pages size_t
   * ms_entries
   */
  data class Stat(
      val pageSize: UInt,
      val treeDepth: UInt,
      val branchPagesCount: Long,
      val leafPagesCount: Long,
      val overflowPagesCount: Long,
      val entriesCount: Long,
  )

  /**
   * Gets the metadata page. Also tries to detect and return the current page size (because that's a
   * function of whatever _SC_PAGESIZE happens to be on the system generating the database. Often
   * it's 4KB, sometimes 16KB)
   *
   * @param testPageSizes a list of page sizes to try
   * @return the parsed metadatapage and the detected pageSize
   */
  private fun getMetadataPage(
      buffer: ByteBuffer,
      testPageSizes: Collection<UInt> = supportedPageSizes,
  ): Pair<Pair<MetaDataPage64, MetaDataPage64>, UInt> {
    testPageSizes.forEach { testPageSize ->
      try {
        logger.trace { "Getting metadata pages with pagesize=$testPageSize" }
        val pages = getMetadataPagesWithPageSize(buffer, testPageSize)
        return Pair(pages, testPageSize)
      } catch (e: Throwable) {
        when (e) {
          is AssertionError,
          is Page.UnsupportedPageTypeException, -> {
            logger.debug { "Page size is not $testPageSize" }
          }
          else -> throw e
        }
      }
    }
    throw UnableToDetectPageSizeException()
  }

  private fun getMetadataPagesWithPageSize(
      buffer: ByteBuffer,
      pageSize: UInt,
  ): Pair<MetaDataPage64, MetaDataPage64> {
    val first =
        try {
          DbMappedBuffer(buffer, pageSize).getPage(0u)
        } catch (e: AssertionError) {
          // If we got to this point on the very first page, then we're not dealing with an LMDB
          // file
          throw NotAnLMDBDataFile()
        }
    assert(first is MetaDataPage64) { "First page is not a metadata page" }
    assert((first as MetaDataPage64).version == 1u) { "Invalid page version ${first.version}" }
    assert(first.magic == 0xBEEFC0DE.toUInt()) { "Page does not contain required magic" }
    val second = DbMappedBuffer(buffer, pageSize).getPage(1u)
    assert(second is MetaDataPage64) { "Second page is not a metadata page" }
    assert((second as MetaDataPage64).version == 1u) { "Invalid page version ${second.version}" }
    assert(second.magic == 0xBEEFC0DE.toUInt()) { "Page does not contain required magic" }
    return Pair(first, second)
  }

  /**
   * A structure representing a byte range in a buffer
   *
   * @property lower
   * @property upper
   */
  data class Range(val lower: UShort, val upper: UShort)

  companion object {
    private const val DATA_FILENAME = "data.mdb"
    private const val LOCK_FILENAME = "lock.mdb"
  }
}

class NotAnLMDBDataFile : Throwable()
