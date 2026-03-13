package com.growse.lmdb_kt

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import io.kotest.core.annotation.Tags
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively
import org.junit.jupiter.api.Tag
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env

/**
 * Coverage-guided fuzz tests. Each invocation:
 * 1. Interprets the fuzz input as a sequence of key-value pairs
 * 2. Writes them into a real LMDB database via lmdbjava
 * 3. Reads them back with lmdb-kt and asserts correctness
 *
 * Run in regression mode (fast, uses saved corpus only):
 * ```
 * just test-integration
 * ```
 *
 * Run with coverage-guided fuzzing (finds new inputs, runs until crash or interrupted):
 * ```
 * just fuzz
 * ```
 */
@Tags("integration")
@Tag("integration")
@OptIn(ExperimentalPathApi::class)
class FuzzTests {
  @FuzzTest(maxDuration = "5m")
  fun fuzz(data: FuzzedDataProvider) {
    val entryCount = data.consumeInt(1, 100)

    // Build a map of unique key→value pairs from the fuzz input.  Keys must be non-empty
    // (LMDB rejects zero-length keys); duplicate keys are silently overwritten.
    val entries = LinkedHashMap<ByteArrayKey, ByteArray>()
    repeat(entryCount) {
      val keySize = data.consumeInt(1, 500) // LMDB max key size is 511 bytes
      val key = data.consumeBytes(keySize)
      if (key.isEmpty()) return // fuzz data exhausted

      val valueSize = data.consumeInt(0, 65_536) // 0 = empty value; >4096 = overflow pages
      val value = data.consumeBytes(valueSize)

      entries[key.toByteArrayKey()] = value
    }

    if (entries.isEmpty()) return

    val dbPath = Files.createTempDirectory("lmdb-kt-fuzz-")
    try {
      // --- Write with lmdbjava (trusted reference implementation) ---
      val estimatedBytes = entries.entries.sumOf { (k, v) -> k.size + v.size + 64L }
      val mapSize = maxOf(10_485_760L, estimatedBytes * 4)

      Env.create().setMapSize(mapSize).setMaxDbs(1).open(dbPath.toFile()).use { env ->
        val db = env.openDbi(null as String?, DbiFlags.MDB_CREATE)
        entries.forEach { (key, value) ->
          val keyBuf = ByteBuffer.allocateDirect(key.size)
          val valueBuf = ByteBuffer.allocateDirect(value.size)
          keyBuf.put(key.bytes).flip()
          if (value.isNotEmpty()) valueBuf.put(value)
          valueBuf.flip()
          db.put(keyBuf, valueBuf)
        }
        db.close()
      }

      // --- Read with lmdb-kt and verify ---
      Environment(
              dbPath,
              readOnly = true,
              locking = false,
              byteOrder = ByteOrder.LITTLE_ENDIAN,
          )
          .use { env ->
            check(env.stat().entriesCount == entries.size.toLong()) {
              "Entry count mismatch: expected ${entries.size}, got ${env.stat().entriesCount}"
            }

            env.beginTransaction().use { tx ->
              // Verify sequential dump matches
              val dump = tx.dump()
              check(dump.size == entries.size) {
                "Dump size mismatch: expected ${entries.size}, got ${dump.size}"
              }

              // Verify every key is reachable via B-tree navigation (get)
              entries.forEach { (key, expectedValue) ->
                val result = tx.get(key.bytes)
                check(result.isSuccess) {
                  "Key not found: ${key.bytes.toHexString()}, " +
                      "error: ${result.exceptionOrNull()?.message}"
                }
                val actualValue = result.getOrThrow()
                check(actualValue.contentEquals(expectedValue)) {
                  "Value mismatch for key ${key.bytes.toHexString()}: " +
                      "expected ${expectedValue.size} bytes, got ${actualValue.size} bytes"
                }
              }
            }
          }
    } finally {
      dbPath.deleteRecursively()
    }
  }
}

private fun ByteArray.toHexString() = joinToString("") { "%02x".format(it) }
