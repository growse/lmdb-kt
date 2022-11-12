package com.growse.lmdb_kt

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource
import org.slf4j.simple.SimpleLogger
import java.nio.file.Paths
import java.security.MessageDigest
import java.util.stream.Stream
import kotlin.io.path.bufferedReader
import kotlin.streams.asSequence
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LmdbTests {
    init {
        System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
    }

    @Test
    fun `given a path that is not a dir, when trying to create an lmdb database, then an assertion error is thrown`() {
        assertThrows<AssertionError> {
            Environment(Paths.get("boop"))
        }
    }

    @ParameterizedTest
    @MethodSource("databasesWithStats")
    fun `given an environment, when fetching the stats, then the correct pagesize is detected`(
        dbPath: String,
        expectedPageSize: Int,
    ) {
        Environment(Paths.get(javaClass.getResource(dbPath)!!.toURI())).use { env ->
            env.stat.run {
                assertEquals(expectedPageSize, pageSize.toInt(), "Page size")
            }
        }
    }


    @ParameterizedTest
    @MethodSource("databasesWithStats")
    fun `given an environment, when fetching the stats, then the correct stats are returned`(
        dbPath: String,
        expectedPageSize: Int,
        expectedTreeDepth: Int,
        expectedBranchPagesCount: Long,
        expectedLeafPagesCount: Long,
        expectedOverflowPagesCount: Long,
        expectedEntriesCount: Long
    ) {
        Environment(Paths.get(javaClass.getResource(dbPath)!!.toURI()), expectedPageSize.toUInt()).use { env ->
            env.stat.run {
                assertEquals(expectedPageSize, pageSize.toInt(), "Page size")
                assertEquals(expectedTreeDepth, treeDepth.toInt(), "Tree depth")
                assertEquals(expectedBranchPagesCount, branchPagesCount, "Branch pages")
                assertEquals(expectedLeafPagesCount, leafPagesCount, "Leaf pages")
                assertEquals(expectedOverflowPagesCount, overflowPagesCount, "Overflow pages")
                assertEquals(expectedEntriesCount, entriesCount, "Entries")
            }
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("databasesWithKeysValues")
    fun `given an environment, when dumping the data, then the correct key-values are returned`(
        dbPath: String, pageSize: Int, expected: Map<String, LengthAndDigest>
    ) {
        Environment(Paths.get(javaClass.getResource(dbPath)!!.toURI()), pageSize.toUInt()).use { env ->
            env.dump().run {
                assertEquals(expected.size, size, "Entry count")
                expected.keys.forEach {
                    assertTrue(keys.contains(it), "Key exists in dump")
                    assertEquals(expected[it]!!.length, this[it]!!.size, "Value size is correct for $it")
                    assertEquals(expected[it]!!.digest, this[it]!!.digest())
                }
            }
        }
    }

    companion object {
        @JvmStatic
        fun databasesWithStats(): Stream<Arguments> =/*
            dbPath: String,
            expectedPageSize: Int,
            expectedTreeDepth: Int,
            expectedBranchPagesCount: Long,
            expectedLeafPagesCount: Long,
            expectedOverflowPagesCount: Long,
            expectedEntriesCount: Long
             */
            Stream.of(
                arguments("/databases/little-endian/16KB-page-size/empty", 16384, 0, 0L, 0L, 0L, 0L),
                arguments("/databases/little-endian/16KB-page-size/single-entry", 16384, 1, 0L, 1L, 0L, 1L),
                arguments("/databases/little-endian/16KB-page-size/single-large-value", 16384, 1, 0L, 1L, 3L, 1L),
                arguments("/databases/little-endian/16KB-page-size/100-random-values", 16384, 2, 1L, 42L, 1L, 100L),
                arguments("/databases/little-endian/4KB-page-size/empty", 4096, 0, 0L, 0L, 0L, 0L),
                arguments("/databases/little-endian/4KB-page-size/single-entry", 4096, 1, 0L, 1L, 0L, 1L),
                arguments("/databases/little-endian/4KB-page-size/single-large-value", 4096, 1, 0L, 1L, 9L, 1L),
                arguments("/databases/little-endian/4KB-page-size/100-random-values", 4096, 3, 3L, 16L, 130L, 100L),
                arguments("/databases/little-endian/4KB-page-size/single-entry-after-deleted", 4096, 1, 0L, 1L, 0L, 1L),
            )

        @JvmStatic
        fun databasesWithKeysValues(): Stream<Arguments> =/*
            dbPath: String, expectedNumberOfEntries: Int, expected
             */
            Stream.of(
                arguments("/databases/little-endian/16KB-page-size/empty", 16384, emptyMap<String, LengthAndDigest>()),
                arguments(
                    "/databases/little-endian/16KB-page-size/single-entry",
                    16384,
                    mapOf("KK123KK" to LengthAndDigest(8, "d2781036b05df83b95dd20f5a497102b"))
                ),
                arguments(
                    "/databases/little-endian/16KB-page-size/single-large-value",
                    16384,
                    mapOf("KK123KK" to LengthAndDigest(33000, "79a965574a648d48fc612f28cc49e570"))
                ),
                arguments("/databases/little-endian/16KB-page-size/100-random-values", 16384, Paths.get(
                    this::class.java.getResource("/100-key-values.csv")!!.toURI()
                ).bufferedReader().lines().asSequence().associate {
                    it.split(",").run {
                        this[0].trim() to LengthAndDigest(this[1].trim().toInt(), this[2].trim())
                    }
                }),
                arguments("/databases/little-endian/4KB-page-size/empty", 4096, emptyMap<String, LengthAndDigest>()),
                arguments(
                    "/databases/little-endian/4KB-page-size/single-entry",
                    4096,
                    mapOf("KK123KK" to LengthAndDigest(8, "d2781036b05df83b95dd20f5a497102b"))
                ),
                arguments(
                    "/databases/little-endian/4KB-page-size/single-large-value",
                    4096,
                    mapOf("KK123KK" to LengthAndDigest(33000, "79a965574a648d48fc612f28cc49e570"))
                ),
                arguments("/databases/little-endian/4KB-page-size/100-random-values", 4096, Paths.get(
                    this::class.java.getResource("/100-key-values.csv")!!.toURI()
                ).bufferedReader().lines().asSequence().associate {
                    it.split(",").run {
                        this[0].trim() to LengthAndDigest(this[1].trim().toInt(), this[2].trim())
                    }
                }),
                arguments(
                    "/databases/little-endian/4KB-page-size/single-entry-after-deleted",
                    4096,
                    mapOf("KK123KK" to LengthAndDigest(8, "d2781036b05df83b95dd20f5a497102b"))
                ),
            )
    }
}

private fun ByteArray.digest(): String = MessageDigest.getInstance("MD5").digest(this).toHex()


data class LengthAndDigest(val length: Int, val digest: String)
