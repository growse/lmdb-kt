package com.growse.lmdb_kt

import io.kotest.core.spec.style.FunSpec
import io.kotest.datatest.withData
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.ints.shouldBeExactly
import java.nio.ByteOrder
import java.nio.file.Paths
import kotlin.io.path.bufferedReader
import kotlin.streams.asSequence
import org.slf4j.simple.SimpleLogger

class LmdbDataDumpTests :
    FunSpec({
      val fixtures =
          listOf(
              DatabaseWithKeysValues(
                  "/databases/little-endian/16KB-page-size/empty",
                  16384,
                  emptyMap(),
              ),
              DatabaseWithKeysValues(
                  "/databases/little-endian/16KB-page-size/single-entry",
                  16384,
                  mapOf(
                      "KK123KK".toByteArrayKey() to
                          LengthAndDigest(8, "d2781036b05df83b95dd20f5a497102b"),
                  ),
              ),
              DatabaseWithKeysValues(
                  "/databases/little-endian/16KB-page-size/single-large-value",
                  16384,
                  mapOf(
                      "KK123KK".toByteArrayKey() to
                          LengthAndDigest(33000, "79a965574a648d48fc612f28cc49e570"),
                  ),
              ),
              DatabaseWithKeysValues(
                  "/databases/little-endian/16KB-page-size/100-random-values",
                  16384,
                  Paths.get(
                          this::class.java.getResource("/100-key-values.csv")!!.toURI(),
                      )
                      .bufferedReader()
                      .lines()
                      .asSequence()
                      .associate {
                        it.split(",").run {
                          this[0].toByteArrayKey() to
                              LengthAndDigest(this[1].trim().toInt(), this[2].trim())
                        }
                      },
              ),
              DatabaseWithKeysValues(
                  "/databases/little-endian/4KB-page-size/empty",
                  4096,
                  emptyMap(),
              ),
              DatabaseWithKeysValues(
                  "/databases/little-endian/4KB-page-size/single-entry",
                  4096,
                  mapOf(
                      "KK123KK".toByteArrayKey() to
                          LengthAndDigest(8, "d2781036b05df83b95dd20f5a497102b"),
                  ),
              ),
              DatabaseWithKeysValues(
                  "/databases/little-endian/4KB-page-size/single-large-value",
                  4096,
                  mapOf(
                      "KK123KK".toByteArrayKey() to
                          LengthAndDigest(33000, "79a965574a648d48fc612f28cc49e570"),
                  ),
              ),
              DatabaseWithKeysValues(
                  "/databases/little-endian/4KB-page-size/100-random-values",
                  4096,
                  Paths.get(
                          this::class.java.getResource("/100-key-values.csv")!!.toURI(),
                      )
                      .bufferedReader()
                      .lines()
                      .asSequence()
                      .associate {
                        it.split(",").run {
                          this[0].toByteArrayKey() to
                              LengthAndDigest(this[1].trim().toInt(), this[2].trim())
                        }
                      },
              ),
              DatabaseWithKeysValues(
                  "/databases/little-endian/4KB-page-size/single-entry-after-deleted",
                  4096,
                  mapOf(
                      "KK123KK".toByteArrayKey() to
                          LengthAndDigest(8, "d2781036b05df83b95dd20f5a497102b"),
                  ),
              ),
              DatabaseWithKeysValues(
                  "/databases/little-endian/android",
                  4096,
                  mapOf(
                      byteArrayOf(0, 0, 0, 0, 0, 0, 0, 0).toByteArrayKey() to
                          LengthAndDigest(
                              224,
                              "92ac74587530bc80322bffe124720811",
                          ),
                      byteArrayOf(0, 0, 0, 0, 0, 0, 0, 1).toByteArrayKey() to
                          LengthAndDigest(
                              744,
                              "989463ef7b13d183dfda5bb08c47d37b",
                          ),
                  ),
              ))
      context(
          "given an environment, when dumping the data, then the correct key-values are returned") {
            withData(nameFn = { it.path }, fixtures) { fixture ->
              run {
                Environment(
                        Paths.get(javaClass.getResource(fixture.path)!!.toURI()),
                        readOnly = true,
                        locking = false,
                        byteOrder = ByteOrder.LITTLE_ENDIAN,
                        pageSize = fixture.pageSize.toUInt(),
                    )
                    .use { env ->
                      env.beginTransaction().use { tx ->
                        tx.dump().run {
                          size shouldBeExactly fixture.data.size
                          keys shouldContainAll fixture.data.keys
                          fixture.data.forEach { fixtureDataValue ->
                            this[fixtureDataValue.key]!!.size shouldBeExactly
                                fixtureDataValue.value.length
                            this[fixtureDataValue.key]!!.digest() shouldBeEqual
                                fixtureDataValue.value.digest
                          }
                        }
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
