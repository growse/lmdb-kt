package com.growse.lmdb_kt

import io.kotest.core.spec.style.FunSpec
import io.kotest.datatest.withData
import io.kotest.matchers.shouldBe
import org.slf4j.simple.SimpleLogger
import java.nio.ByteOrder
import java.nio.file.Paths

class LmdbDataStatsTests :
    FunSpec({
      val fixtures =
          listOf(
              DatabaseWithStats(
                  "/databases/little-endian/16KB-page-size/empty",
                  16384,
                  0,
                  0L,
                  0L,
                  0L,
                  0L,
              ),
              DatabaseWithStats(
                  "/databases/little-endian/16KB-page-size/single-entry",
                  16384,
                  1,
                  0L,
                  1L,
                  0L,
                  1L,
              ),
              DatabaseWithStats(
                  "/databases/little-endian/16KB-page-size/single-large-value",
                  16384,
                  1,
                  0L,
                  1L,
                  3L,
                  1L,
              ),
              DatabaseWithStats(
                  "/databases/little-endian/16KB-page-size/100-random-values",
                  16384,
                  2,
                  1L,
                  42L,
                  1L,
                  100L,
              ),
              DatabaseWithStats(
                  "/databases/little-endian/4KB-page-size/empty",
                  4096,
                  0,
                  0L,
                  0L,
                  0L,
                  0L,
              ),
              DatabaseWithStats(
                  "/databases/little-endian/4KB-page-size/single-entry",
                  4096,
                  1,
                  0L,
                  1L,
                  0L,
                  1L,
              ),
              DatabaseWithStats(
                  "/databases/little-endian/4KB-page-size/single-large-value",
                  4096,
                  1,
                  0L,
                  1L,
                  9L,
                  1L,
              ),
              DatabaseWithStats(
                  "/databases/little-endian/4KB-page-size/100-random-values",
                  4096,
                  3,
                  3L,
                  16L,
                  130L,
                  100L,
              ),
              DatabaseWithStats(
                  "/databases/little-endian/4KB-page-size/single-entry-after-deleted",
                  4096,
                  1,
                  0L,
                  1L,
                  0L,
                  1L,
              ),
              DatabaseWithStats("/databases/little-endian/android", 4096, 1, 0L, 1L, 0L, 2L),
          )
      context("Stat produces the correct page size") {
        withData(nameFn = { it.dbPath }, fixtures) { pathAndStat ->
          run {
            Environment(
                Paths.get(javaClass.getResource(pathAndStat.dbPath)!!.toURI()),
                readOnly = true,
                locking = false,
                byteOrder = ByteOrder.LITTLE_ENDIAN,
            )
                .use { env ->
                  env.stat().run { pageSize.toInt() shouldBe pathAndStat.expectedPageSize }
                }
          }
        }
      }
      context("Stat produces the correct stats") {
        withData(nameFn = { it.dbPath }, fixtures) { pathAndStat ->
          run {
            Environment(
                Paths.get(javaClass.getResource(pathAndStat.dbPath)!!.toURI()),
                readOnly = true,
                locking = false,
                byteOrder = ByteOrder.LITTLE_ENDIAN,
                pageSize = pathAndStat.expectedPageSize.toUInt(),
            )
                .use { env ->
                  env.stat().run {
                    pageSize.toInt() shouldBe pathAndStat.expectedPageSize
                    treeDepth.toInt() shouldBe pathAndStat.expectedTreeDepth
                    branchPagesCount shouldBe pathAndStat.expectedBranchPagesCount
                    leafPagesCount shouldBe pathAndStat.expectedLeafPagesCount
                    entriesCount shouldBe pathAndStat.expectedEntriesCount
                  }
                }
          }
        }
      }
    }) {
  init {
    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE")
  }
}
