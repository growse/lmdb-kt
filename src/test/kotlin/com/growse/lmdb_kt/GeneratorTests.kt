//package com.growse.lmdb_kt
//
//import java.nio.ByteBuffer.allocateDirect
//import java.nio.ByteOrder
//import java.nio.file.Files
//import java.nio.file.Path
//import java.util.stream.Stream
//import kotlin.io.path.ExperimentalPathApi
//import kotlin.io.path.deleteRecursively
//import kotlin.io.path.exists
//import kotlin.test.assertContains
//import kotlin.test.assertEquals
//import kotlin.test.assertTrue
//import org.junit.jupiter.api.Named.named
//import org.junit.jupiter.params.ParameterizedTest
//import org.junit.jupiter.params.provider.Arguments
//import org.junit.jupiter.params.provider.Arguments.arguments
//import org.junit.jupiter.params.provider.MethodSource
//import org.lmdbjava.DbiFlags
//import org.lmdbjava.Env
//import org.slf4j.simple.SimpleLogger
//
///** Creates an LMDB using the native implementation and then attempts to read with ours */
//class GeneratorTests {
//  init {
//    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE")
//  }
//
//  @ParameterizedTest(name = "{index}: {1}")
//  @MethodSource("databasesGeneratedWithReferenceImplementation")
//  fun `given an environment, when fetching the stats, then the correct pagesize is detected`(
//      databasePath: Path,
//      fixtureData: Map<ByteArrayKey, ByteArray>
//  ) {
//    Environment(
//            databasePath,
//            readOnly = true,
//            locking = false,
//            byteOrder = ByteOrder.LITTLE_ENDIAN,
//        )
//        .use { env ->
//          assertEquals(fixtureData.count().toLong(), env.stat().entriesCount)
//
//          env.beginTransaction().use { tx ->
//            // First we check the dump is the same
//            val databaseDump = tx.dump()
//            assertEquals(fixtureData.size, databaseDump.size)
//            databaseDump.forEach {
//              assertContains(fixtureData, it.key)
//              assertTrue(fixtureData[it.key].contentEquals(it.value))
//            }
//            // Also test we can fetch every key in the fixture
//            fixtureData.forEach {
//              val fetchedValue = tx.get(it.key.bytes)
//              assertTrue(fetchedValue.isSuccess)
//              assertTrue(it.value.contentEquals(fetchedValue.getOrThrow()))
//            }
//          }
//        }
//  }
//
//  companion object {
//    @OptIn(ExperimentalPathApi::class)
//    @JvmStatic
//    fun databasesGeneratedWithReferenceImplementation(): Stream<Arguments> {
//
//      val data = mutableMapOf<ByteArrayKey, ByteArray>()
//      (1..20).forEach { data["TestKey$it".toByteArrayKey()] = "TestValue$it".toByteArray() }
//      Path.of("/tmp/lmdb-test").let { dbPath ->
//        if (dbPath.exists()) {
//          dbPath.deleteRecursively()
//        }
//        val tempDir = Files.createDirectory(dbPath)
//
//        val env = Env.create().setMapSize(10_485_760).setMaxDbs(1).open(tempDir.toFile())
//        val name: String? = null
//        val db = env.openDbi(name, DbiFlags.MDB_CREATE)
//        data.forEach {
//          val key = allocateDirect(env.maxKeySize)
//          val value = allocateDirect(1_024)
//          key.put(it.key.bytes).flip()
//          value.put(it.value).flip()
//          db.put(key, value)
//        }
//        db.close()
//        return Stream.of(arguments(tempDir, named("TestPrefixedData", data)))
//      }
//    }
//  }
//}
