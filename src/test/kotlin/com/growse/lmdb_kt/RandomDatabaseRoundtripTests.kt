package com.growse.lmdb_kt

import io.kotest.core.spec.style.FreeSpec
import io.kotest.datatest.withData
import io.kotest.matchers.maps.shouldHaveSize
import io.kotest.matchers.result.shouldBeSuccess
import io.kotest.matchers.shouldBe
import java.nio.ByteBuffer.allocateDirect
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively
import kotlin.random.Random
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env

/**
 * Parameterized round-trip test suite. For each configuration, a fresh LMDB database is created
 * using the reference lmdbjava implementation with random key-value pairs, then read back using
 * lmdb-kt. The full dump and individual key lookups are both verified.
 */
@OptIn(ExperimentalPathApi::class)
class RandomDatabaseRoundtripTests :
    FreeSpec({
      // Parameterize over database configurations
      withData(
          nameFn = { it.description },
          DatabaseConfig(
              entryCount = 1,
              pageSize = 4_096,
              maxValueSize = 512,
              seed = 1L,
              description = "4KB pages, 1 entry, small values",
          ),
          DatabaseConfig(
              entryCount = 10,
              pageSize = 4_096,
              maxValueSize = 512,
              seed = 2L,
              description = "4KB pages, 10 entries, small values",
          ),
          DatabaseConfig(
              entryCount = 20,
              pageSize = 4_096,
              maxValueSize = 512,
              seed = 12L,
              description = "4KB pages, 20 entries, small values",
          ),
          DatabaseConfig(
              entryCount = 100,
              pageSize = 4_096,
              maxValueSize = 2_048,
              seed = 3L,
              description = "4KB pages, 100 entries, mixed values",
          ),
          DatabaseConfig(
              entryCount = 500,
              pageSize = 4_096,
              maxValueSize = 2_048,
              seed = 4L,
              description = "4KB pages, 500 entries, mixed values",
          ),
          DatabaseConfig(
              entryCount = 10,
              pageSize = 4_096,
              maxValueSize = 40_000,
              seed = 5L,
              description = "4KB pages, 10 entries, large overflow values",
          ),
          DatabaseConfig(
              entryCount = 100,
              pageSize = 4_096,
              maxValueSize = 40_000,
              seed = 6L,
              description = "4KB pages, 100 entries, large overflow values",
          ),
          DatabaseConfig(
              entryCount = 1,
              pageSize = 16_384,
              maxValueSize = 512,
              seed = 7L,
              description = "16KB pages, 1 entry, small values",
          ),
          DatabaseConfig(
              entryCount = 100,
              pageSize = 16_384,
              maxValueSize = 4_096,
              seed = 8L,
              description = "16KB pages, 100 entries, mixed values",
          ),
          DatabaseConfig(
              entryCount = 500,
              pageSize = 16_384,
              maxValueSize = 4_096,
              seed = 9L,
              description = "16KB pages, 500 entries, mixed values",
          ),
          DatabaseConfig(
              entryCount = 100,
              pageSize = 16_384,
              maxValueSize = 100_000,
              seed = 10L,
              description = "16KB pages, 100 entries, large overflow values",
          ),
          DatabaseConfig(
              entryCount = 50,
              pageSize = 4_096,
              maxValueSize = 512,
              seed = 11L,
              description = "4KB pages, 50 entries, binary keys",
              binaryKeys = true,
          ),
      ) { config ->
        val dbPath = Files.createTempDirectory("lmdb-kt-roundtrip-")
        try {
          val fixture = createDatabase(dbPath, config)

          Environment(
                  dbPath,
                  readOnly = true,
                  locking = false,
                  byteOrder = ByteOrder.LITTLE_ENDIAN,
              )
              .use { env ->
                env.stat().entriesCount shouldBe fixture.size.toLong()

                env.beginTransaction().use { tx ->
                  // Verify the full dump matches — dump() walks all pages sequentially
                  val dump = tx.dump()
                  dump shouldHaveSize fixture.size
                  fixture.forEach { (key, expectedValue) ->
                    val dumpValue = dump[key]
                    dumpValue?.toList() shouldBe expectedValue.toList()
                  }

                  // Verify each key can be found via B-tree navigation (get)
                  fixture.forEach { (key, expectedValue) ->
                    val result = tx.get(key.bytes)
                    result.shouldBeSuccess { value -> value.toList() shouldBe expectedValue.toList() }
                  }
                }
              }
        } finally {
          dbPath.deleteRecursively()
        }
      }
    })

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

data class DatabaseConfig(
    val entryCount: Int,
    val pageSize: Int,
    val maxValueSize: Int,
    val seed: Long,
    val description: String,
    val binaryKeys: Boolean = false,
)

/**
 * Creates an LMDB database at [dbPath] using the reference lmdbjava implementation. Returns the
 * map of key-value pairs written, keyed by [ByteArrayKey] for easy comparison.
 */
private fun createDatabase(
    dbPath: Path,
    config: DatabaseConfig,
): Map<ByteArrayKey, ByteArray> {
  val rng = Random(config.seed)

  // Generate unique keys up-front (lmdb is a map; duplicate keys would reduce entry count)
  val data = mutableMapOf<ByteArrayKey, ByteArray>()
  while (data.size < config.entryCount) {
    val key =
        if (config.binaryKeys) {
          rng.nextBytes(rng.nextInt(1, 32))
        } else {
          // Prefix keeps keys human-readable and avoids accidental duplicates
          "KEY-${rng.nextLong(Long.MAX_VALUE)}-${rng.nextInt()}".toByteArray()
        }
    val valueSize = rng.nextInt(1, config.maxValueSize + 1)
    data[key.toByteArrayKey()] = rng.nextBytes(valueSize)
  }

  // Map size must comfortably fit all data
  val estimatedBytes =
      data.entries.sumOf { (k, v) -> k.size + v.size + 64L /* per-entry overhead */ }
  val mapSize = maxOf(10_485_760L, estimatedBytes * 4)

  Env.create()
      .setMapSize(mapSize)
      .setMaxDbs(1)
      .open(dbPath.toFile())
      .use { env ->
        val db = env.openDbi(null as String?, DbiFlags.MDB_CREATE)
        data.forEach { (key, value) ->
          val keyBuf = allocateDirect(env.maxKeySize)
          val valueBuf = allocateDirect(value.size)
          keyBuf.put(key.bytes).flip()
          valueBuf.put(value).flip()
          db.put(keyBuf, valueBuf)
        }
        db.close()
      }

  return data
}
