package com.growse.lmdb_kt

import com.growse.lmdb_kt.Page.KeyNotFoundInPage
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import java.nio.ByteOrder
import java.nio.file.Paths
import org.slf4j.simple.SimpleLogger

@OptIn(ExperimentalStdlibApi::class)
class LmdbTests :
    BehaviorSpec({
      Given("a path that is not a directory") {
        When("loading the environment") {
          Then("an assertion error is thrown") {
            shouldThrow<AssertionError> { Environment(Paths.get("boop")) }
          }
        }
      }
      Given("a path that doesn't contain a valid database file") {
        When("loading the environment") {
          Then("a NotAnLMDBDataFile exception is thrown") {
            shouldThrow<NotAnLMDBDataFile> {
              Environment(
                      Paths.get(
                          javaClass
                              .getResource("/databases/little-endian/not-a-database")!!
                              .toURI(),
                      ),
                      readOnly = true,
                      locking = false,
                      byteOrder = ByteOrder.LITTLE_ENDIAN,
                      pageSize = 4096.toUInt(),
                  )
                  .use {}
            }
          }
        }
      }
      Given("a given a valid database environment") {
        When("fetching the stats with the wrong pagesize") {
          Then("an assertion error is thrown") {
            shouldThrow<UnableToDetectPageSizeException> {
              Environment(
                      Paths.get(
                          javaClass
                              .getResource(
                                  "/databases/little-endian/4KB-page-size/100-random-values")!!
                              .toURI(),
                      ),
                      readOnly = true,
                      locking = false,
                      byteOrder = ByteOrder.LITTLE_ENDIAN,
                      pageSize = 1024.toUInt(),
                  )
                  .use { env -> env.stat() }
            }
          }
        }

        When("getting an oversize value for a key") {
          Then("the value is returned") {
            val key = "KEYimcfsuuqqdufeckfbglgoairkcfhvwsafzwmbpgfbxzhtvlrx"
            Environment(
                    Paths.get(
                        javaClass
                            .getResource(
                                "/databases/little-endian/4KB-page-size/100-random-values")!!
                            .toURI(),
                    ),
                    readOnly = true,
                    locking = false,
                    byteOrder = ByteOrder.LITTLE_ENDIAN,
                    pageSize = 4096.toUInt(),
                )
                .use { env ->
                  env.beginTransaction().use { tx ->
                    val value = tx.get(key.toByteArray())
                    assert(value.isSuccess)
                    value.getOrThrow().run {
                      size shouldBeExactly 7209
                      digest() shouldBe "f161ed45d7744c25a2ffd85c828c0543"
                    }
                  }
                }
          }
        }
        When("getting an undersized value for a key") {
          Then("the value is returned") {
            val key = "KEYb"
            Environment(
                    Paths.get(
                        javaClass
                            .getResource(
                                "/databases/little-endian/4KB-page-size/100-random-values")!!
                            .toURI(),
                    ),
                    readOnly = true,
                    locking = false,
                    byteOrder = ByteOrder.LITTLE_ENDIAN,
                    pageSize = 4096.toUInt(),
                )
                .use { env ->
                  env.beginTransaction().use { tx ->
                    val value = tx.get(key.toByteArray())
                    assert(value.isSuccess)
                    value.getOrThrow().run {
                      size shouldBeExactly 419
                      digest() shouldBe "b7506a2d4d442dac673c46d27a20d1f7"
                    }
                  }
                }
          }
        }
        When("getting a value for a key that doesn't exist") {
          Then("a failure result is returned") {
            val key = "Non-existent"
            Environment(
                    Paths.get(
                        javaClass
                            .getResource(
                                "/databases/little-endian/4KB-page-size/100-random-values")!!
                            .toURI(),
                    ),
                    readOnly = true,
                    locking = false,
                    byteOrder = ByteOrder.LITTLE_ENDIAN,
                    pageSize = 4096.toUInt(),
                )
                .use { env ->
                  env.beginTransaction().use { tx ->
                    val value = tx.get(key.toByteArray())
                    value.isFailure shouldBe true
                    value.exceptionOrNull().shouldBeInstanceOf<KeyNotFoundInPage>()
                    value.exceptionOrNull().toString() shouldContain key.toByteArray().toHexString()
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

//
//  @ParameterizedTest(name = "{index}: {0}")
//  @MethodSource("databasesWithStats")
//  fun `given an environment, when fetching the stats, then the correct pagesize is detected`(
//      databasePathWithStats: DatabaseWithStats,
//  ) {
//    Environment(
//            Paths.get(javaClass.getResource(databasePathWithStats.dbPath)!!.toURI()),
//            readOnly = true,
//            locking = false,
//            byteOrder = ByteOrder.LITTLE_ENDIAN,
//        )
//        .use { env ->
//          env.stat().run {
//            assertEquals(databasePathWithStats.expectedPageSize, pageSize.toInt(), "Page size")
//          }
//        }
//  }
//

//
//  @ParameterizedTest(name = "{index}: {0}")
//  @MethodSource("databasesWithStats")
//  fun `given an environment, when fetching the stats, then the correct stats are returned`(
//      databaseWithStats: DatabaseWithStats,
//  ) {
//    Environment(
//            Paths.get(javaClass.getResource(databaseWithStats.dbPath)!!.toURI()),
//            readOnly = true,
//            locking = false,
//            byteOrder = ByteOrder.LITTLE_ENDIAN,
//            pageSize = databaseWithStats.expectedPageSize.toUInt(),
//        )
//        .use { env ->
//          env.stat().run {
//            assertEquals(databaseWithStats.expectedPageSize, pageSize.toInt(), "Page size")
//            assertEquals(databaseWithStats.expectedTreeDepth, treeDepth.toInt(), "Tree depth")
//            assertEquals(
//                databaseWithStats.expectedBranchPagesCount,
//                branchPagesCount,
//                "Branch pages",
//            )
//            assertEquals(databaseWithStats.expectedLeafPagesCount, leafPagesCount, "Leaf pages")
//            assertEquals(
//                databaseWithStats.expectedOverflowPagesCount,
//                overflowPagesCount,
//                "Overflow pages",
//            )
//            assertEquals(databaseWithStats.expectedEntriesCount, entriesCount, "Entries")
//          }
//        }
//  }
//
//  @ParameterizedTest(name = "{index}: {0}")
//  @MethodSource("databasesWithKeysValues")
//  fun `given an environment, when dumping the data, then the correct key-values are returned`(
//      dbPath: String,
//      pageSize: Int,
//      expected: Map<ByteArrayKey, LengthAndDigest>,
//  ) {
//    Environment(
//            Paths.get(javaClass.getResource(dbPath)!!.toURI()),
//            readOnly = true,
//            locking = false,
//            byteOrder = ByteOrder.LITTLE_ENDIAN,
//            pageSize = pageSize.toUInt(),
//        )
//        .use { env ->
//          env.beginTransaction().use { tx ->
//            tx.dump().run {
//              assertEquals(expected.size, size, "Entry count")
//              expected.keys.forEach {
//                assertTrue(keys.contains(it), "Key exists in dump")
//                assertEquals(
//                    expected[it]!!.length,
//                    this[it]!!.size,
//                    "Value size is correct for $it",
//                )
//                assertEquals(expected[it]!!.digest, this[it]!!.digest())
//              }
//            }
//          }
//        }
//  }
//

//
//  }
//

//
//  data class DatabaseWithStats(
//      val dbPath: String,
//      val expectedPageSize: Int,
//      val expectedTreeDepth: Int,
//      val expectedBranchPagesCount: Long,
//      val expectedLeafPagesCount: Long,
//      val expectedOverflowPagesCount: Long,
//      val expectedEntriesCount: Long,
//  )
//
//  companion object {
//    /** Test parameters for each test database, listing the path and the stats for that db */
//    @JvmStatic
//    fun databasesWithStats(): Stream<DatabaseWithStats> =
//        Stream.of(
//            DatabaseWithStats(
//                "/databases/little-endian/16KB-page-size/empty",
//                16384,
//                0,
//                0L,
//                0L,
//                0L,
//                0L,
//            ),
//            DatabaseWithStats(
//                "/databases/little-endian/16KB-page-size/single-entry",
//                16384,
//                1,
//                0L,
//                1L,
//                0L,
//                1L,
//            ),
//            DatabaseWithStats(
//                "/databases/little-endian/16KB-page-size/single-large-value",
//                16384,
//                1,
//                0L,
//                1L,
//                3L,
//                1L,
//            ),
//            DatabaseWithStats(
//                "/databases/little-endian/16KB-page-size/100-random-values",
//                16384,
//                2,
//                1L,
//                42L,
//                1L,
//                100L,
//            ),
//            DatabaseWithStats(
//                "/databases/little-endian/4KB-page-size/empty",
//                4096,
//                0,
//                0L,
//                0L,
//                0L,
//                0L,
//            ),
//            DatabaseWithStats(
//                "/databases/little-endian/4KB-page-size/single-entry",
//                4096,
//                1,
//                0L,
//                1L,
//                0L,
//                1L,
//            ),
//            DatabaseWithStats(
//                "/databases/little-endian/4KB-page-size/single-large-value",
//                4096,
//                1,
//                0L,
//                1L,
//                9L,
//                1L,
//            ),
//            DatabaseWithStats(
//                "/databases/little-endian/4KB-page-size/100-random-values",
//                4096,
//                3,
//                3L,
//                16L,
//                130L,
//                100L,
//            ),
//            DatabaseWithStats(
//                "/databases/little-endian/4KB-page-size/single-entry-after-deleted",
//                4096,
//                1,
//                0L,
//                1L,
//                0L,
//                1L,
//            ),
//            DatabaseWithStats("/databases/little-endian/android", 4096, 1, 0L, 1L, 0L, 2L),
//        )
//
//    /** Test parameters for each test database, the path and the values that are contained */
//    @JvmStatic
//    fun databasesWithKeysValues(): Stream<Arguments> =
//        /*
//                dbPath: String,
//                expectedNumberOfEntries: Int,
//                expected
//        */
//        Stream.of(
//            arguments(
//                "/databases/little-endian/16KB-page-size/empty",
//                16384,
//                emptyMap<String, LengthAndDigest>(),
//            ),
//            arguments(
//                "/databases/little-endian/16KB-page-size/single-entry",
//                16384,
//                mapOf(
//                    "KK123KK".toByteArrayKey() to
//                        LengthAndDigest(8, "d2781036b05df83b95dd20f5a497102b"),
//                ),
//            ),
//            arguments(
//                "/databases/little-endian/16KB-page-size/single-large-value",
//                16384,
//                mapOf(
//                    "KK123KK".toByteArrayKey() to
//                        LengthAndDigest(33000, "79a965574a648d48fc612f28cc49e570"),
//                ),
//            ),
//            arguments(
//                "/databases/little-endian/16KB-page-size/100-random-values",
//                16384,
//                Paths.get(
//                        this::class.java.getResource("/100-key-values.csv")!!.toURI(),
//                    )
//                    .bufferedReader()
//                    .lines()
//                    .asSequence()
//                    .associate {
//                      it.split(",").run {
//                        this[0].toByteArrayKey() to
//                            LengthAndDigest(this[1].trim().toInt(), this[2].trim())
//                      }
//                    },
//            ),
//            arguments(
//                "/databases/little-endian/4KB-page-size/empty",
//                4096,
//                emptyMap<String, LengthAndDigest>(),
//            ),
//            arguments(
//                "/databases/little-endian/4KB-page-size/single-entry",
//                4096,
//                mapOf(
//                    "KK123KK".toByteArrayKey() to
//                        LengthAndDigest(8, "d2781036b05df83b95dd20f5a497102b"),
//                ),
//            ),
//            arguments(
//                "/databases/little-endian/4KB-page-size/single-large-value",
//                4096,
//                mapOf(
//                    "KK123KK".toByteArrayKey() to
//                        LengthAndDigest(33000, "79a965574a648d48fc612f28cc49e570"),
//                ),
//            ),
//            arguments(
//                "/databases/little-endian/4KB-page-size/100-random-values",
//                4096,
//                Paths.get(
//                        this::class.java.getResource("/100-key-values.csv")!!.toURI(),
//                    )
//                    .bufferedReader()
//                    .lines()
//                    .asSequence()
//                    .associate {
//                      it.split(",").run {
//                        this[0].toByteArrayKey() to
//                            LengthAndDigest(this[1].trim().toInt(), this[2].trim())
//                      }
//                    },
//            ),
//            arguments(
//                "/databases/little-endian/4KB-page-size/single-entry-after-deleted",
//                4096,
//                mapOf(
//                    "KK123KK".toByteArrayKey() to
//                        LengthAndDigest(8, "d2781036b05df83b95dd20f5a497102b"),
//                ),
//            ),
//            arguments(
//                "/databases/little-endian/android",
//                4096,
//                mapOf(
//                    byteArrayOf(0, 0, 0, 0, 0, 0, 0, 0).toByteArrayKey() to
//                        LengthAndDigest(
//                            224,
//                            "92ac74587530bc80322bffe124720811",
//                        ),
//                    byteArrayOf(0, 0, 0, 0, 0, 0, 0, 1).toByteArrayKey() to
//                        LengthAndDigest(
//                            744,
//                            "989463ef7b13d183dfda5bb08c47d37b",
//                        ),
//                ),
//            ),
//        )
//  }
// }
