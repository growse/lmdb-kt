package com.growse.lmdb_kt

import com.growse.lmdb_kt.Environment.Stat
import mu.KotlinLogging
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.MappedByteBuffer
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
class Environment(lmdbPath: Path, pageSize: UInt? = null, readOnly: Boolean = false, locking: Boolean = true) :
    AutoCloseable {
    private val logger = KotlinLogging.logger {}
    private val fileChannel: FileChannel
    private val mapped: MappedByteBuffer
    private val pageSize: UInt
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
        val dataFile = lmdbPath.resolve(DATA_FILENAME)
        val lockFile = lmdbPath.resolve(LOCK_FILENAME)
        assert(lmdbPath.isDirectory()) { "Supplied path is not a directory" }
        assert(dataFile.isRegularFile()) { "Supplied path does not contain a data file" }
        assert(lockFile.isRegularFile()) { "Supplied path does not contain a lock file" }

        logger.trace { "Mapping file $dataFile" }
        fileChannel = FileChannel.open(dataFile)
        mapped = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataFile.fileSize())

        val (metadata, detectedPageSize) = if (pageSize != null) {
            getMetadataPage(listOf(pageSize))
        } else {
            getMetadataPage()
        }

        this.pageSize = detectedPageSize
        this.metadata = metadata
        this.stat = Stat(
            detectedPageSize,
            metadata.mainDb.depth.toUInt(),
            metadata.mainDb.branchPages,
            metadata.mainDb.leafPages,
            metadata.mainDb.overflowPages,
            metadata.mainDb.entries
        )

        assert(dataFile.fileSize() % detectedPageSize.toInt() == 0L) { "Data file is not a valid size" }
        // TODO work out what the right lock file size should be...? It's 8KB on a 16KB pagesize db
//        assert(lockFile.fileSize() % pageSize.toInt() == 0L) { "Lock file is not a valid size" }

        val rootPageNumber = metadata.mainDb.rootPageNumber
        if (rootPageNumber >= 0) { // -1 is an empty db
            assert(rootPageNumber <= UInt.MAX_VALUE.toLong())
        }
    }

    override fun close() {
        logger.trace { "Close" }
        if (fileChannel.isOpen) {
            fileChannel.close()
        }
    }

    /**
     * Dumps all keys/values out
     *
     * @return a map of keys (as strings) and values
     */
    fun dump(): Map<String, ByteArray> {
        if (this.metadata.mainDb.rootPageNumber == -1L) {
            return emptyMap()
        }
        val rootPage = getPage(this.metadata.mainDb.rootPageNumber.toUInt())
        return dumpPage(rootPage)
    }

    /**
     * Dumps the keys/values from a specific page. Walks through to child pages
     *
     * @param page the page to dump data for
     * @return a map of keys/values
     */
    private fun dumpPage(page: Page): Map<String, ByteArray> {
        when (page) {
            is LeafPage -> {
                return page.nodes.associate { leafNode ->
                    when (leafNode.value) {
                        is Either.Left -> {
                            String(leafNode.key) to leafNode.value.left
                        }

                        // It's an overflow value
                        is Either.Right -> {
                            val overflowPage = getPage(leafNode.value.right.toUInt())
                            assert(overflowPage is OverflowPage)
                            String(leafNode.key) to (overflowPage as OverflowPage).getValue(
                                leafNode.valueSize,
                                this.pageSize,
                                mapped
                            )
                        }
                    }
                }
            }

            is BranchPage -> {
                return page.nodes.map { nodeAddress ->
                    logger.trace { "Branch node points to page at ${nodeAddress.childPage}" }
                    dumpPage(getPage(nodeAddress.childPage, pageSize))
                }.fold(mutableMapOf()) { acc, map -> acc.apply { putAll(map) } }
            }

            else -> TODO("Not implemented")
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
    private fun getMetadataPage(testPageSizes: Collection<UInt> = supportedPageSizes): Pair<MetaDataPage64, UInt> {
        testPageSizes.forEach { testPageSize ->
            try {
                val page = getMetadataPageWithPageSize(testPageSize)
                return Pair(page, testPageSize)
            } catch (e: Throwable) {
                when (e) {
                    is AssertionError, is UnsupportedPageTypeException -> {
                        logger.debug { "Page size is not $testPageSize" }
                    }

                    else -> throw e
                }
            }
        }
        throw UnableToDetectPageSizeException()
    }

    private fun getMetadataPageWithPageSize(pageSize: UInt): MetaDataPage64 {
        val first = getPage(0u, pageSize)
        assert(first is MetaDataPage64) { "First page is not a metadata page" }
        val second = getPage(1u, pageSize)
        assert(second is MetaDataPage64) { "Second page is not a metadata page" }
        val selectedPage =
            if ((first as MetaDataPage64).txnId > (second as MetaDataPage64).txnId) first else second
        assert(selectedPage.magic == 0xBEEFC0DE.toUInt()) { "Page does not contain required magic. Instead ${selectedPage.magic}" }
        assert(selectedPage.version == 1u) { "Invalid page version ${selectedPage.version}" } // Not supporting development version 999
        logger.trace { "Page size is $pageSize" }
        return selectedPage
    }

    /**
     * Parses the given page number into the correct structure
     *
     * @param number the page number to parse
     * @param pageSize the pageSize of the database
     * @return the parsed page
     */
    private fun getPage(number: UInt, pageSize: UInt = this.pageSize): Page {
        logger.trace { "Getting page $number" }
        val pageBuffer = ByteBuffer.wrap(getRawPage(number, pageSize)).also { it.order(BYTE_ORDER) }
        val pageHeader = PageHeader(pageBuffer)
        return if (pageHeader.flags.contains(Flags.Page.META)) {
            pageBuffer.position(PageHeader.SIZE.toInt())
            MetaDataPage64(pageBuffer)
        } else if (pageHeader.flags.contains(Flags.Page.LEAF)) {
            LeafPage(pageHeader, pageBuffer)
        } else if (pageHeader.flags.contains(Flags.Page.OVERFLOW)) {
            OverflowPage(pageHeader)
        } else if (pageHeader.flags.contains(Flags.Page.BRANCH)) {
            BranchPage(pageHeader, pageBuffer)
        } else {
            throw UnsupportedPageTypeException(pageHeader.flags)
        }
    }

    interface Node

    /**
     * A structure that represents a single key/value pair. Bundled together to form a [LeafPage]. Each node can either
     * contain the key and value contiguously if they fit within the space available in the page, or with the [NODE_BIGDATA]
     * flag set the node will point towards another [OverflowPage] which contains the value.
     *
     * @constructor
     * Parses the node data from the buffer
     *
     * @param buffer a [ByteBuffer] that contains the Node
     */
    class LeafNode(
        buffer: ByteBuffer
    ) : Node {
        private val logger = KotlinLogging.logger {}
        val lo: UShort
        val hi: UShort
        val flags: EnumSet<Flags.Node>
        val kSize: UShort
        val valueSize: UInt
        val key: ByteArray

        val value: Either<ByteArray, Long> // The value is either a bytearray or a reference to an overflow page

        init {
            logger.trace { "Parsing leaf node at ${buffer.position()}" }
            lo = buffer.short.toUShort()
            hi = buffer.short.toUShort()
            flags = Flags.fromBuffer(Flags.Node::class.java, buffer, 2u)
            kSize = buffer.short.toUShort()
            logger.trace { "Key is $kSize bytes" }
            logger.trace { "Reading key at ${buffer.position()}" }
            key = ByteArray(kSize.toInt()).apply(buffer::get)
            logger.trace { "Key value is ${key.toHex()} or ${key.toAscii()}" }
            valueSize = lo + (hi.toUInt().shl(16))

            value = if (flags.contains(Flags.Node.BIGDATA)) {
                Either.Right(buffer.long.also { logger.trace { "Value is bigdata at page $it" } })
            } else {
                logger.trace { "Value is $valueSize bytes at ${buffer.position()}" }
                Either.Left(
                    ByteArray(valueSize.toInt()).apply(buffer::get)
                        .also { logger.trace { "Value is ${it.toHex()} or ${it.toAscii()}" } }
                )
            }
        }
    }

    data class BranchNode(val buffer: ByteBuffer) : Node {
        private val logger = KotlinLogging.logger {}
        val lo: UShort
        val hi: UShort
        val flags: BitSet
        val kSize: UShort
        val key: ByteArray
        val childPage: UInt

        init {
            logger.trace { "Parsing branch node at ${buffer.position()}" }
            lo = buffer.short.toUShort()
            hi = buffer.short.toUShort()
            flags = ByteArray(2).let {
                buffer.get(it)
                BitSet.valueOf(it)
            }
            kSize = buffer.short.toUShort()
            logger.trace { "Key is $kSize bytes" }
            logger.trace { "Reading key at ${buffer.position()}" }
            key = ByteArray(kSize.toInt()).apply(buffer::get)
            logger.trace { "Key value is ${key.toHex()} or ${key.toAscii()}" }
            childPage = lo + (hi.toUInt().shl(16))
            logger.trace { "Child page is at $childPage" }
        }
    }

    interface Page
    class EmptyPage : Page

    interface MetaDataPage : Page

    /**
     * MDB_meta
     * uint32_t 	        mm_magic
     * uint32_t 	        mm_version
     * void * 	            mm_address
     * size_t 	            mm_mapsize
     * MDB_db 	            mm_dbs [CORE_DBS] (CORE_DBS   2 - Number of DBs in metapage (free and main) - also hardcoded elsewhere)
     * pgno_t 	            mm_last_pg
     * volatile txnid_t 	mm_txnid
     */
    data class MetaDataPage64(
        val magic: UInt,
        val version: UInt,
        val address: ULong,
        val mapSize: ULong,
        val freeDb: DB,
        val mainDb: DB,
        val lastPage: ULong,
        val txnId: ULong
    ) : MetaDataPage {
        constructor(buffer: ByteBuffer) : this(
            magic = buffer.int.toUInt(),
            version = buffer.int.toUInt(),
            address = buffer.long.toULong(),
            mapSize = buffer.long.toULong(),
            freeDb = DB(buffer),
            mainDb = DB(buffer),
            lastPage = buffer.long.toULong(),
            txnId = buffer.long.toULong()
        )
    }

    /**
     * A Leaf page contains a bunch of [LeafNode]s.
     *
     * @property pageHeader the page header
     * @property nodes a list of [LeafNode]
     */
    data class LeafPage(
        val pageHeader: PageHeader,
        val nodes: List<LeafNode>
    ) : Page {
        constructor(pageHeader: PageHeader, buffer: ByteBuffer) : this(
            pageHeader,
            nodes = IntRange(1, pageHeader.numKeys())
                .map { buffer.short }
                .map {
                    buffer.position(it.toInt())
                    LeafNode(buffer)
                }
        )
    }

    data class BranchPage(
        val pageHeader: PageHeader,
        val nodes: List<BranchNode>
    ) : Page {
        constructor(pageHeader: PageHeader, buffer: ByteBuffer) : this(
            pageHeader,
            nodes = IntRange(1, pageHeader.numKeys())
                .map { buffer.short }
                .map {
                    buffer.position(it.toInt())
                    BranchNode(buffer)
                }
        )
    }

    data class OverflowPage(val pageHeader: PageHeader) : Page {
        /**
         * Gets the value of an overflow page. Overflow values start at an overflow page, and then run through contiguous
         * pages
         *
         * @param valueSize the size of the overflow value to read. Should be less thon number of overflow pages * pageSize
         * @param pageSize the size of a page
         * @param mappedBuffer the memory-map
         * @return the value from reading the page (and any subsequent)
         */
        fun getValue(valueSize: UInt, pageSize: UInt, mappedBuffer: MappedByteBuffer): ByteArray {
            assert(pageHeader.pagesOrRange is Either.Left) { "Overflow page does not have pageCount value" }
            val numberOfPages = (pageHeader.pagesOrRange as Either.Left).left
            assert(valueSize < numberOfPages * pageSize) { "Requested value size is bigger than fits in number of pages" }
            return ByteArray(valueSize.toInt()).apply {
                mappedBuffer.position(((pageHeader.pageNumber * pageSize.toInt()) + PageHeader.SIZE.toInt()).toInt())
                mappedBuffer.get(this)
            }
        }
    }

    /**
     * Represents a single database
     * http://www.lmdb.tech/doc/group__internal.html#structMDB__db
     *  uint32_t 	md_pad
     *  uint16_t 	md_flags
     *  uint16_t 	md_depth
     *  pgno_t 	    md_branch_pages
     *  pgno_t 	    md_leaf_pages
     *  pgno_t 	    md_overflow_pages
     *  size_t 	    md_entries
     *  pgno_t 	    md_root
     */
    data class DB(
        val pad: Int,
        val flags: EnumSet<Flags.Db>,
        val depth: Short,
        val branchPages: Long,
        val leafPages: Long,
        val overflowPages: Long,
        val entries: Long,
        val rootPageNumber: Long
    ) {
        constructor(buffer: ByteBuffer) : this(
            pad = buffer.int,
            flags = Flags.fromBuffer(Flags.Db::class.java, buffer, 2u),
            depth = buffer.short,
            branchPages = buffer.long,
            leafPages = buffer.long,
            overflowPages = buffer.long,
            entries = buffer.long,
            rootPageNumber = buffer.long
        )
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

    /**
     * Represents a page header
     * http://www.lmdb.tech/doc/group__internal.html#structMDB__page
     *
     *  union {
     *      pgno_t   p_pgno
     *      struct MDB_page *   p_next
     *  } 	mp_p
     *  uint16_t 	mp_pad
     *  uint16_t 	mp_flags
     *  union {
     *      struct {
     *          indx_t   pb_lower
     *          indx_t   pb_upper
     *      }   pb
     *      uint32_t   pb_pages
     *  } 	mp_pb
     *  indx_t 	mp_ptrs
     *
     * @constructor
     * Parses the page header, determining whether or not the page is an overflow
     *
     * @param buffer a [ByteBuffer] for the page
     */
    class PageHeader(buffer: ByteBuffer) {
        val pageNumber: Long
        val padding: UShort
        val flags: EnumSet<Flags.Page>
        val pagesOrRange: Either<UInt, Range>

        companion object {
            const val SIZE: UInt = 16u
        }

        init {
            pageNumber = buffer.long
            padding = buffer.short.toUShort()
            flags = Flags.fromBuffer(Flags.Page::class.java, buffer, 2u)
            pagesOrRange = if (flags.contains(Flags.Page.OVERFLOW)) {
                Either.Left(buffer.int.toUInt())
            } else {
                Either.Right(Range(buffer.short.toUShort(), buffer.short.toUShort()))
            }
        }

        fun numKeys(): Int = when (pagesOrRange) {
            is Either.Left -> 0
            is Either.Right -> (pagesOrRange.right.lower.toShort() - 16) / 2
        }
    }

    /**
     * Copies a page out of the memory map into a bytearray
     *
     * @param number the number of the page to copy
     * @param pageSize the size of the page for the database
     * @return a byte array representing the page
     */
    private fun getRawPage(number: UInt, pageSize: UInt): ByteArray = mapped.run {
        ByteArray(pageSize.toInt()).also {
            position(number.toInt() * pageSize.toInt())
            get(it)
        }
    }

    companion object {
        private const val DATA_FILENAME = "data.mdb"
        private const val LOCK_FILENAME = "lock.mdb"

        private val BYTE_ORDER = ByteOrder.LITTLE_ENDIAN
    }
}
