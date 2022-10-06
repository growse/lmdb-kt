package com.growse.lmdb_kt

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

class Environment(lmdbPath: Path) : AutoCloseable {
    private val logger = KotlinLogging.logger {}
    private val fileChannel: FileChannel
    private val mapped: MappedByteBuffer
    private val pageSize: UInt
    private val supportedPageSizes = listOf(4u * 1024u, 8u * 1024u, 16u * 1024u)
    val stat: Stat

    init {
        val dataFile = lmdbPath.resolve(DATA_FILENAME)
        val lockFile = lmdbPath.resolve(LOCK_FILENAME)
        assert(lmdbPath.isDirectory()) { "Supplied path is not a directory" }
        assert(dataFile.isRegularFile()) { "Supplied path does not contain a data file" }
        assert(lockFile.isRegularFile()) { "Supplied path does not contain a lock file" }


        fileChannel = FileChannel.open(dataFile)
        mapped = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataFile.fileSize())

        val (metadata, pageSize) = getMetadataPage()
        this.pageSize = pageSize
        this.stat = Stat(
            pageSize,
            metadata.mainDb.depth.toUInt(),
            metadata.mainDb.branchPages,
            metadata.mainDb.leafPages,
            metadata.mainDb.overflowPages,
            metadata.mainDb.entries
        )

        assert(dataFile.fileSize() % pageSize.toInt() == 0L) { "Data file is not a valid size" }
        // TODO work out what the right lock file size should be...? It's 8KB on a 16KB pagesize db
//        assert(lockFile.fileSize() % pageSize.toInt() == 0L) { "Lock file is not a valid size" }

        val rootPageNumber = metadata.mainDb.rootPageNumber
        assert(rootPageNumber > 0)
        assert(rootPageNumber <= UInt.MAX_VALUE.toLong())
        logger.trace { "Fetching root page" }
        val rootPage = getPage(rootPageNumber.toUInt())

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
     */
    private fun getMetadataPage(): Pair<MetaDataPage64, UInt> {
        supportedPageSizes.forEach { testPageSize ->
            try {
                val first = getPage(0u, testPageSize)
                assert(first is MetaDataPage64) { "First page is not a metadata page" }
                val second = getPage(1u, testPageSize)
                assert(second is MetaDataPage64) { "Second page is not a metadata page" }
                val selectedPage =
                    if ((first as MetaDataPage64).txnId > (second as MetaDataPage64).txnId) first else second
                assert(selectedPage.magic == 0xBEEFC0DE.toUInt()) { "Page does not contain required magic. Instead ${selectedPage.magic}" }
                assert(selectedPage.version == 1u || selectedPage.version == 999u) { "Invalid page version ${selectedPage.version}" }
                logger.trace { "Page size is $testPageSize" }
                return Pair(selectedPage, testPageSize)
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

    private fun getPage(number: UInt, pageSize: UInt = this.pageSize): Page {
        logger.trace { "Getting page $number" }
        val pageBuffer = ByteBuffer.wrap(getRawPage(number, pageSize)).also { it.order(BYTE_ORDER) }
        val pageHeader = PageHeader(pageBuffer)
        return if (pageHeader.flags.get(PAGE_META)) {
            pageBuffer.position(PAGE_HEADER_SIZE)
            MetaDataPage64(pageBuffer)
        } else if (pageHeader.flags.get(PAGE_LEAF)) {
            LeafPage(pageHeader, pageBuffer)
        } else if (pageHeader.flags.get(PAGE_OVERFLOW)) {
            EmptyPage()
        } else if (pageHeader.flags.get(PAGE_BRANCH)) {
            throw UnsupportedPageTypeException(pageHeader.flags)
        } else {
            throw UnsupportedPageTypeException(pageHeader.flags)
        }
    }


    interface Node

    // http://www.lmdb.tech/doc/group__internal.html#structMDB__node
    class LeafNode(
        buffer: ByteBuffer
    ) : Node {
        private val logger = KotlinLogging.logger {}
        val lo: UShort
        val hi: UShort
        val flags: BitSet
        val kSize: UShort
        val key: ByteArray
        val value: ByteArray

        init {
            logger.trace { "Parsing leaf node at ${buffer.position()}" }
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
            val valueSize = lo + (hi.toUInt().shl(16))

            //TODO BigData
            value = if (flags.get(NODE_BIGDATA)) {
                buffer.long.also { logger.trace { "Value is bigdata at address $it" } }
                ByteArray(0)
            } else {
                logger.trace { "Value is $valueSize bytes at ${buffer.position()}" }
                ByteArray(valueSize.toInt()).apply(buffer::get)
            }
            logger.trace { "Value is ${value.toHex()} or ${value.toAscii()}" }
        }
    }

    private fun getRawPage(number: UInt, pageSize: UInt): ByteArray = mapped.run {
        ByteArray(pageSize.toInt()).also {
            position(number.toInt() * pageSize.toInt())
            get(it)
        }
    }


    fun keys(): Iterable<ByteArray> {
        TODO()
    }

    override fun close() {
        if (fileChannel.isOpen) {
            fileChannel.close()
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
        val txnId: ULong,
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

    //
    /**
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
        val flags: BitSet,
        val depth: Short,
        val branchPages: Long,
        val leafPages: Long,
        val overflowPages: Long,
        val entries: Long,
        val rootPageNumber: Long,
    ) {
        constructor(buffer: ByteBuffer) : this(
            pad = buffer.int,
            flags = BitSet.valueOf(ByteArray(2).apply(buffer::get)),
            depth = buffer.short,
            branchPages = buffer.long,
            leafPages = buffer.long,
            overflowPages = buffer.long,
            entries = buffer.long,
            rootPageNumber = buffer.long
        )

    }

    data class Range(
        val lower: UShort,
        val upper: UShort
    )

    class PageHeader(buffer: ByteBuffer) {
        val pageNumber: Long
        val padding: UShort
        val flags: BitSet
        val pagesOrRange: Either<Int, Range>

        init {
            pageNumber = buffer.long
            padding = buffer.short.toUShort()
            flags = BitSet.valueOf(ByteArray(2).apply(buffer::get))
            pagesOrRange = if (flags.get(PAGE_OVERFLOW)) {
                Either.Left(buffer.int)
            } else {
                Either.Right(Range(buffer.short.toUShort(), buffer.short.toUShort()))
            }
        }

        fun numKeys(): Int = when (pagesOrRange) {
            is Either.Left -> 0
            is Either.Right -> (pagesOrRange.right.lower.toShort() - 16) / 2
        }
    }

    companion object {
        private const val DATA_FILENAME = "data.mdb"
        private const val LOCK_FILENAME = "lock.mdb"

        private val BYTE_ORDER = ByteOrder.LITTLE_ENDIAN

        private const val PAGE_HEADER_SIZE = 16

        // Page Flags http://www.lmdb.tech/doc/group__mdb__page.html
        private const val PAGE_BRANCH = 0
        private const val PAGE_LEAF = 1
        private const val PAGE_OVERFLOW = 2
        private const val PAGE_META = 3
        private const val PAGE_DIRTY = 4
        private const val PAGE_LEAF2 = 5
        private const val PAGE_SUBP = 6
        private const val PAGE_LOOSE = 14
        private const val PAGE_KEEP = 15

        private const val MDFLAGS_REVERSEKEY = 0
        private const val MDFLAGS_DUPSORT = 1
        private const val MDFLAGS_INTEGERKEY = 2
        private const val MDFLAGS_DUPFIXED = 3
        private const val MDFLAGS_INTEGERDUP = 4
        private const val MDFLAGS_REVERSEDUP = 5
        private const val MDFLAGS_CREATE = 14

        // Node Flags http://www.lmdb.tech/doc/group__mdb__node.html
        private const val NODE_BIGDATA = 0
        private const val NODE_SUBDATA = 1
        private const val NODE_DUPDATA = 2
    }
}
