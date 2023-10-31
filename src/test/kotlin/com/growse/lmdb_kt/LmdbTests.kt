package com.growse.lmdb_kt

import com.growse.lmdb_kt.Page.KeyNotFoundInPage
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import org.slf4j.simple.SimpleLogger
import java.nio.ByteOrder
import java.nio.file.Paths

// Fixture types
data class DatabaseWithStats(
    val dbPath: String,
    val expectedPageSize: Int,
    val expectedTreeDepth: Int,
    val expectedBranchPagesCount: Long,
    val expectedLeafPagesCount: Long,
    val expectedOverflowPagesCount: Long,
    val expectedEntriesCount: Long,
)

data class DatabaseWithKeysValues(
    val path: String,
    val pageSize: Int,
    val data: Map<ByteArrayKey, LengthAndDigest>
)

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
      Given("a valid database environment") {
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
