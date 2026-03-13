package com.growse.lmdb_kt

import kotlin.random.Random
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test

class UtilityTests {
  private val random = Random(1)
  private val b1 = ByteArray(50).run(random::nextBytes)
  private val b2 = ByteArray(85).run(random::nextBytes)

  @Test
  fun `given two bytearrays that are the same, when comparing them then the result is 0`() {
    assertEquals(0, b1.compareWith(b1))
  }

  @Test
  fun `given two bytearrays that are different, when comparing them lexicographically, then the result reflects unsigned byte order`() {
    // b2 is lexicographically greater than b1 under unsigned (LMDB-style) comparison
    assertEquals(1, b2.compareWith(b1))
  }

  @Test
  fun `given two bytearrays that are different, when comparing them in reverse, then the result reflects unsigned byte order`() {
    // b1 is lexicographically less than b2 under unsigned (LMDB-style) comparison
    assertEquals(-1, b1.compareWith(b2))
  }

  @Test
  fun `given two bytearrays where one is a prefix, when comparing shorter with longer, then the result is -1`() {
    assertEquals(-1, byteArrayOf(1, 2, 3).compareWith(byteArrayOf(1, 2, 3, 4)))
  }

  @Test
  fun `given two bytearrays where one is a prefix, when comparing longer with shorter, then the result is 1`() {
    assertEquals(1, byteArrayOf(1, 2, 3, 4).compareWith(byteArrayOf(1, 2, 3)))
  }

  @Test
  fun `given bytes across the 0x7F boundary, compareWith uses unsigned ordering`() {
    // 0x80 (128 unsigned) > 0x7F (127 unsigned) — correct LMDB/unsigned order
    assertEquals(1, byteArrayOf(0x80.toByte()).compareWith(byteArrayOf(0x7F)))
    assertEquals(-1, byteArrayOf(0x7F).compareWith(byteArrayOf(0x80.toByte())))
  }
}
