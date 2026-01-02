package com.growse.lmdb_kt

import io.kotest.core.spec.style.StringSpec
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.checkAll
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.createTempDirectory
import kotlin.io.path.deleteRecursively
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env
import org.lmdbjava.Txn

@OptIn(kotlin.io.path.ExperimentalPathApi::class)
class PropertyBasedInteropTest :
    StringSpec({
      fun randomBytes(size: Int): ByteArray {
        val arr = ByteArray(size)
        Random.nextBytes(arr)
        return arr
      }

      fun randomKeyValues(
          count: Int,
          maxKeySize: Int,
          maxValSize: Int
      ): Map<ByteArrayKey, ByteArray> {
        val map = LinkedHashMap<ByteArrayKey, ByteArray>()
        repeat(count) {
          val keyLen = Random.nextInt(1, maxKeySize)
          val valLen = Random.nextInt(0, maxValSize)
          val key = randomBytes(keyLen)
          val value = randomBytes(valLen)
          map[ByteArrayKey(key)] = value
        }
        return map
      }

      fun writeWithNativeLmdb(dir: Path, entries: Map<ByteArrayKey, ByteArray>) {
        val env = Env.create().setMapSize(16 * 1024 * 1024).setMaxDbs(1).open(dir.toFile())
        val dbi = env.openDbi(null as ByteArray?, DbiFlags.MDB_CREATE)
        env.txnWrite().use { txn: Txn<ByteBuffer> ->
          entries.forEach { (k, v) ->
            val kBuf = ByteBuffer.allocateDirect(k.bytes.size)
            kBuf.put(k.bytes).flip()
            val vBuf = ByteBuffer.allocateDirect(v.size)
            vBuf.put(v).flip()
            dbi.put(txn, kBuf, vBuf)
          }
          txn.commit()
        }
        dbi.close()
        env.close()
      }

      fun assertLibraryReads(dir: Path, expected: Map<ByteArrayKey, ByteArray>) {
        Environment(dir, readOnly = true, locking = false).use { env ->
          val dump = env.beginTransaction().dump()
          // Ensure at least keys count matches
          assertEquals(expected.size, dump.size, "Entry count mismatch")
          // Validate each value by key lookup
          expected.forEach { (k, v) ->
            val got = env.beginTransaction().get(k.bytes)
            assertTrue(got.isSuccess, "Missing key ${k.bytes.toPrintableString()}")
            assertEquals(
                v.toList(),
                got.getOrThrow().toList(),
                "Value mismatch for key ${k.bytes.toPrintableString()}")
          }
        }
      }

      "random kv roundtrip with varying counts" {
        checkAll(Arb.int(1..100)) { count ->
          val dir = createTempDirectory("lmdbkt-prop-")
          try {
            val entries = randomKeyValues(count = count, maxKeySize = 64, maxValSize = 512)
            writeWithNativeLmdb(dir, entries)
            assertLibraryReads(dir, entries)
          } finally {
            Files.list(dir).forEach { it.toFile().delete() }
            dir.deleteRecursively()
          }
        }
      }

      "duplicates last write wins" {
        val dir = createTempDirectory("lmdbkt-prop-dup-")
        try {
          val baseEntries = randomKeyValues(count = 50, maxKeySize = 32, maxValSize = 1024)
          val duplicateKeys = baseEntries.keys.take(10)
          val finalEntries = LinkedHashMap<ByteArrayKey, ByteArray>()
          finalEntries.putAll(baseEntries)
          duplicateKeys.forEach { k -> finalEntries[k] = randomBytes(256) }
          writeWithNativeLmdb(dir, finalEntries)
          assertLibraryReads(dir, finalEntries)
        } finally {
          Files.list(dir).forEach { it.toFile().delete() }
          dir.deleteRecursively()
        }
      }
    })
