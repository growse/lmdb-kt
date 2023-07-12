package com.growse.lmdb_kt

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.stream.Stream
import kotlin.test.assertEquals

class DbMappedBufferTests {
	@ParameterizedTest(name = "{0} = {1}")
	@MethodSource("dbFlags")
	fun `given a flags bitset, when parsing it, we get the right flags out`(
		bitset: Short,
		expected: Set<DB.Flags>,
	) {
		val parsedFlags =
			DbMappedBuffer(
				ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort(bitset).rewind(),
				2u,
			)
				.flags(DB.Flags::class.java, 2u)
		assertEquals(expected, parsedFlags.toSet())
	}

	companion object {

		/** Test parameters for each test database, listing the path and the stats for that db */
		@JvmStatic
		fun dbFlags(): Stream<Arguments> =
			Stream.of(
				// Basics
				Arguments.of(0.s, emptySet<DB.Flags>()),
				Arguments.of(1.s, setOf(DB.Flags.REVERSEKEY)),
				Arguments.of(2.s, setOf(DB.Flags.DUPSORT)),
				Arguments.of(3.s, setOf(DB.Flags.DUPSORT, DB.Flags.REVERSEKEY)),
				Arguments.of(4.s, setOf(DB.Flags.INTEGERKEY)),
				Arguments.of(5.s, setOf(DB.Flags.INTEGERKEY, DB.Flags.REVERSEKEY)),
				Arguments.of(6.s, setOf(DB.Flags.INTEGERKEY, DB.Flags.DUPSORT)),
				Arguments.of(7.s, setOf(DB.Flags.INTEGERKEY, DB.Flags.DUPSORT, DB.Flags.REVERSEKEY)),
				Arguments.of(8.s, setOf(DB.Flags.DUPFIXED)),
				Arguments.of(16384.s, setOf(DB.Flags.CREATE)),
				// Ignored bits
				Arguments.of(64.s, emptySet<DB.Flags>()),
			)
		private val Int.s: Short
			get() = toShort()
	}
}
