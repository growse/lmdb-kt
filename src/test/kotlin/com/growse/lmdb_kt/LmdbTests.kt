package com.growse.lmdb_kt

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.slf4j.simple.SimpleLogger
import java.nio.file.Paths
import kotlin.test.assertEquals

class LmdbTests {
    init {
        System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
    }

    @Test
    fun `given a small lmdb database, when trying to stat, then the correct environment stats are returned`() {
        Environment(
            Paths.get(
                javaClass.getResource("/databases/little-endian/16KB-page-size/single-entry")!!.toURI()
            )
        ).use { env ->
            env.stat.run {
                assertEquals(16384u, pageSize, "Page size is 16KB")
                assertEquals(1u, treeDepth, "Depth of 1")
                assertEquals(0, branchPagesCount, "0 branch pages")
                assertEquals(1, leafPagesCount, "1 leaf page")
                assertEquals(0, overflowPagesCount, "0 overflow pages")
                assertEquals(1, entriesCount, "1 entry")
            }
        }
    }

    @Test
    fun `given a path that is not a dir, when trying to create an lmdb database, then an assertion error is thrown`() {
        assertThrows<AssertionError> {
            Environment(Paths.get("boop"))
        }
    }
}
