package com.growse.lmdb_kt.benchmark

import com.growse.lmdb_kt.Environment
import com.growse.lmdb_kt.Transaction
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocateDirect
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively
import kotlin.random.Random
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.annotations.Warmup
import org.openjdk.jmh.infra.Blackhole

/**
 * Benchmarks comparing random-read and full-scan throughput between:
 *  - lmdbjava: the standard JNI-backed native LMDB binding
 *  - lmdb-kt:  the pure-Kotlin LMDB reader
 *
 * Both implementations read from the same database file created by lmdbjava during setup.
 *
 * Run with:
 *   ./gradlew :benchmark:jmh
 *
 * Narrow to a specific benchmark:
 *   ./gradlew :benchmark:jmh -Pjmh.includes="randomRead.*"
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
open class LmdbBenchmarks {

  @Param("1000", "10000") var entryCount: Int = 1000

  private lateinit var dbPath: Path
  private lateinit var keys: List<ByteArray>
  private var mapSize: Long = 0

  // lmdbjava (native JNI binding)
  private lateinit var nativeEnv: Env<ByteBuffer>
  private lateinit var nativeKeyBuf: ByteBuffer

  // lmdb-kt (pure-Kotlin reader)
  private lateinit var ktEnvironment: Environment
  private lateinit var ktTransaction: Transaction

  private var keyIndex = 0

  @OptIn(ExperimentalPathApi::class)
  @Setup(Level.Trial)
  fun setup() {
    dbPath = Files.createTempDirectory("lmdb-bench-")

    val rng = Random(42)
    val data =
        (1..entryCount).associate {
          "KEY-${it.toString().padStart(8, '0')}".toByteArray() to rng.nextBytes(512)
        }
    keys = data.keys.toList()

    val estimatedBytes =
        data.entries.sumOf { (k, v) -> k.size.toLong() + v.size.toLong() + 64L }
    mapSize = maxOf(10_485_760L, estimatedBytes * 4)

    // Populate the database using lmdbjava
    Env.create().setMapSize(mapSize).setMaxDbs(1).open(dbPath.toFile()).use { env ->
      val db = env.openDbi(null as String?, DbiFlags.MDB_CREATE)
      data.forEach { (key, value) ->
        val keyBuf = allocateDirect(key.size)
        val valueBuf = allocateDirect(value.size)
        keyBuf.put(key).flip()
        valueBuf.put(value).flip()
        db.put(keyBuf, valueBuf)
      }
      db.close()
    }

    // Open lmdbjava for reading
    nativeEnv = Env.create().setMapSize(mapSize).setMaxDbs(1).open(dbPath.toFile())
    nativeKeyBuf = allocateDirect(nativeEnv.maxKeySize)

    // Open lmdb-kt for reading
    ktEnvironment =
        Environment(dbPath, readOnly = true, locking = false, byteOrder = ByteOrder.LITTLE_ENDIAN)
    ktTransaction = ktEnvironment.beginTransaction()
  }

  @OptIn(ExperimentalPathApi::class)
  @TearDown(Level.Trial)
  fun teardown() {
    ktTransaction.close()
    ktEnvironment.close()
    nativeEnv.close()
    dbPath.deleteRecursively()
  }

  /** Look up a single key via lmdbjava's JNI binding. */
  @Benchmark
  fun randomReadNative(bh: Blackhole) {
    val key = nextKey()
    nativeEnv.txnRead().use { txn ->
      val db = nativeEnv.openDbi(null as String?)
      nativeKeyBuf.clear()
      nativeKeyBuf.put(key).flip()
      bh.consume(db.get(txn, nativeKeyBuf))
    }
  }

  /** Look up a single key via the pure-Kotlin lmdb-kt reader. */
  @Benchmark
  fun randomReadKt(bh: Blackhole) {
    bh.consume(ktTransaction.getBuffer(nextKey()).getOrThrow())
  }

  /** Iterate every entry in the database using lmdbjava's cursor. */
  @Benchmark
  fun sequentialScanNative(bh: Blackhole) {
    nativeEnv.txnRead().use { txn ->
      val db = nativeEnv.openDbi(null as String?)
      db.iterate(txn).use { cursor ->
        for (kv in cursor) {
          bh.consume(kv.key())
          bh.consume(kv.`val`())
        }
      }
    }
  }

  /** Iterate every entry in the database using lmdb-kt's dump(). */
  @Benchmark
  fun sequentialScanKt(bh: Blackhole) {
    bh.consume(ktTransaction.dump())
  }

  /** Iterate every entry in the database using lmdb-kt's zero-copy streaming scan. */
  @Benchmark
  fun sequentialScanKtStream(bh: Blackhole) {
    ktTransaction.scan().forEach { (key, value) ->
      bh.consume(key)
      bh.consume(value)
    }
  }

  private fun nextKey(): ByteArray {
    val key = keys[keyIndex]
    keyIndex = (keyIndex + 1) % keys.size
    return key
  }
}
