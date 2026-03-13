package com.growse.lmdb_kt

import io.kotest.core.annotation.Tags
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
 *
 * Note: lmdbjava creates databases with the OS-default page size (4096 bytes on Linux); page size
 * is auto-detected when opening with lmdb-kt.
 */
@Tags("integration")
@OptIn(ExperimentalPathApi::class)
class RandomDatabaseRoundtripTests :
    FreeSpec({
      withData(
          nameFn = { it.description },

          // -- ASCII string keys ----------------------------------------------------------
          DatabaseConfig(1, 512, seed = 1, "1 entry, small value"),
          DatabaseConfig(10, 512, seed = 2, "10 entries, small values"),
          DatabaseConfig(20, 512, seed = 12, "20 entries, small values"),
          DatabaseConfig(50, 512, seed = 20, "50 entries, small values"),
          DatabaseConfig(100, 2_048, seed = 3, "100 entries, mixed values"),
          DatabaseConfig(100, 2_048, seed = 30, "100 entries, mixed values (seed 30)"),
          DatabaseConfig(500, 2_048, seed = 4, "500 entries, mixed values"),
          DatabaseConfig(500, 2_048, seed = 40, "500 entries, mixed values (seed 40)"),
          DatabaseConfig(1_000, 512, seed = 19, "1000 entries, small values -- deep tree"),
          DatabaseConfig(2_000, 512, seed = 25, "2000 entries, small values -- deep tree"),

          // -- Overflow values (value > page size -> spans multiple pages) ---------------
          DatabaseConfig(10, 40_000, seed = 5, "10 entries, large overflow values"),
          DatabaseConfig(100, 40_000, seed = 6, "100 entries, large overflow values"),
          DatabaseConfig(500, 40_000, seed = 50, "500 entries, large overflow values"),

          // -- Empty / zero-length values ------------------------------------------------
          DatabaseConfig(50, 0, seed = 21, "50 entries, all empty values", allowEmptyValues = true),
          DatabaseConfig(
              100,
              1_024,
              seed = 22,
              "100 entries, mixed empty and non-empty values",
              allowEmptyValues = true,
          ),

          // -- Binary random keys (uniform bytes, 1-32 bytes) ----------------------------
          DatabaseConfig(
              50, 512, seed = 11, "50 entries, binary random keys", KeyStyle.BINARY_RANDOM),
          DatabaseConfig(
              200, 512, seed = 23, "200 entries, binary random keys", KeyStyle.BINARY_RANDOM),
          DatabaseConfig(
              500, 512, seed = 24, "500 entries, binary random keys", KeyStyle.BINARY_RANDOM),

          // -- High-byte binary keys (all bytes in 0x80-0xFF) ----------------------------
          // These stress the unsigned-vs-signed byte comparison in B-tree navigation.
          DatabaseConfig(
              50, 512, seed = 13, "50 entries, high-byte binary keys", KeyStyle.BINARY_HIGH),
          DatabaseConfig(
              200, 512, seed = 14, "200 entries, high-byte binary keys", KeyStyle.BINARY_HIGH),
          DatabaseConfig(
              500, 1_024, seed = 26, "500 entries, high-byte binary keys", KeyStyle.BINARY_HIGH),

          // -- Common-prefix keys --------------------------------------------------------
          // Many keys share a 20-byte prefix -- stresses B-tree navigation when adjacent
          // keys are nearly identical.
          DatabaseConfig(
              100, 1_024, seed = 15, "100 entries, common-prefix keys", KeyStyle.COMMON_PREFIX),
          DatabaseConfig(
              500, 1_024, seed = 16, "500 entries, common-prefix keys", KeyStyle.COMMON_PREFIX),
          DatabaseConfig(
              1_000,
              512,
              seed = 27,
              "1000 entries, common-prefix keys -- deep tree",
              KeyStyle.COMMON_PREFIX),

          // -- Near-max-length keys (~460-480 bytes, limit is 511) -----------------------
          DatabaseConfig(
              50, 512, seed = 17, "50 entries, near-max-length keys", KeyStyle.NEAR_MAX_LENGTH),
          DatabaseConfig(
              100,
              40_000,
              seed = 18,
              "100 entries, near-max-length keys, overflow values",
              KeyStyle.NEAR_MAX_LENGTH),
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
                  // Verify the full dump matches -- dump() walks all pages sequentially
                  val dump = tx.dump()
                  dump shouldHaveSize fixture.size
                  fixture.forEach { (key, expectedValue) ->
                    dump[key]?.toList() shouldBe expectedValue.toList()
                  }

                  // Verify each key can be found via B-tree navigation (get)
                  fixture.forEach { (key, expectedValue) ->
                    val result = tx.get(key.bytes)
                    result.shouldBeSuccess { value ->
                      value.toList() shouldBe expectedValue.toList()
                    }
                  }
                }
              }
        } finally {
          dbPath.deleteRecursively()
        }
      }
    })

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Controls how key bytes are generated for a test database. */
enum class KeyStyle {
  /** ASCII string "KEY-<long>-<int>" -- all bytes in printable range. */
  STRING,

  /** Uniform random bytes, 1-32 bytes long. */
  BINARY_RANDOM,

  /**
   * Random bytes all in [0x80, 0xFF], 2-16 bytes long. Stresses unsigned byte comparison in B-tree
   * branch navigation.
   */
  BINARY_HIGH,

  /**
   * Fixed 20-byte random prefix followed by a unique 4-byte big-endian counter. Stresses B-tree
   * navigation when many adjacent keys share a long common prefix.
   */
  COMMON_PREFIX,

  /**
   * Random-content keys of 460-480 bytes, approaching the 511-byte LMDB key-size limit. Tests that
   * large keys are stored and retrieved correctly.
   */
  NEAR_MAX_LENGTH,
}

data class DatabaseConfig(
    val entryCount: Int,
    val maxValueSize: Int,
    val seed: Long,
    val description: String,
    val keyStyle: KeyStyle = KeyStyle.STRING,
    /** When true, values may be zero-length (empty ByteArray). */
    val allowEmptyValues: Boolean = false,
)

// ---------------------------------------------------------------------------
// Database creation
// ---------------------------------------------------------------------------

/**
 * Creates an LMDB database at [dbPath] using the reference lmdbjava implementation. Returns the map
 * of key-value pairs written, keyed by [ByteArrayKey] for easy comparison.
 */
private fun createDatabase(
    dbPath: Path,
    config: DatabaseConfig,
): Map<ByteArrayKey, ByteArray> {
  val rng = Random(config.seed)

  // For COMMON_PREFIX, generate the shared prefix once from the seed.
  val commonPrefix: ByteArray? =
      if (config.keyStyle == KeyStyle.COMMON_PREFIX) rng.nextBytes(20) else null

  val data = mutableMapOf<ByteArrayKey, ByteArray>()
  var counter = 0
  while (data.size < config.entryCount) {
    val key =
        when (config.keyStyle) {
          KeyStyle.STRING -> "KEY-${rng.nextLong(Long.MAX_VALUE)}-${rng.nextInt()}".toByteArray()
          KeyStyle.BINARY_RANDOM -> rng.nextBytes(rng.nextInt(1, 33))
          KeyStyle.BINARY_HIGH -> {
            val size = rng.nextInt(2, 17)
            // Map each random value into [0x80, 0xFF] so all bytes have the high bit set.
            ByteArray(size) { (0x80 or rng.nextInt(0x80)).toByte() }
          }
          KeyStyle.COMMON_PREFIX -> commonPrefix!! + counter.toBigEndianBytes()
          KeyStyle.NEAR_MAX_LENGTH -> rng.nextBytes(rng.nextInt(460, 481))
        }
    val minValue = if (config.allowEmptyValues) 0 else 1
    val valueSize =
        if (config.maxValueSize == 0) 0 else rng.nextInt(minValue, config.maxValueSize + 1)
    data[key.toByteArrayKey()] = rng.nextBytes(valueSize)
    counter++
  }

  // Map size must comfortably accommodate all data.
  val estimatedBytes =
      data.entries.sumOf { (k, v) -> k.size + v.size + 64L /* per-entry overhead */ }
  val mapSize = maxOf(10_485_760L, estimatedBytes * 4)

  Env.create().setMapSize(mapSize).setMaxDbs(1).open(dbPath.toFile()).use { env ->
    val db = env.openDbi(null as String?, DbiFlags.MDB_CREATE)
    data.forEach { (key, value) ->
      val keyBuf = allocateDirect(key.size)
      val valueBuf = allocateDirect(value.size)
      keyBuf.put(key.bytes).flip()
      if (value.isNotEmpty()) valueBuf.put(value)
      valueBuf.flip()
      db.put(keyBuf, valueBuf)
    }
    db.close()
  }

  return data
}

/** Encodes this [Int] as 4 big-endian bytes, matching LMDB's default integer key ordering. */
private fun Int.toBigEndianBytes(): ByteArray =
    ByteArray(4) { i -> (this ushr (24 - 8 * i)).toByte() }
