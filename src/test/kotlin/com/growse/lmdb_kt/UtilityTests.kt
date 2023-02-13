package com.growse.lmdb_kt

import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertEquals

class UtilityTests {
	private val random = Random(1)
	private val b1 = ByteArray(50).run(random::nextBytes)
	private val b2 = ByteArray(85).run(random::nextBytes)

	@Test
	fun `given two bytearrays that are the same, when comparing them then the result is 0`() {
		assertEquals(0, b1.compareWith(b1))
	}

	@Test
	fun `given two bytearrays that are different, when comparing the larger with the smaller, then the result is -1`() {
		assertEquals(-1, b2.compareWith(b1))
	}

	@Test
	fun `given two bytearrays that are the same, when comparing the smaller with the larger, then the result is 1`() {
		assertEquals(1, b1.compareWith(b2))
	}
}
