package com.growse.lmdb_kt

import java.nio.ByteBuffer
import java.util.*

class Flags {

    /**
     * Page flags
     * http://www.lmdb.tech/doc/group__mdb__page.html
     */
    enum class Page(val _idx: Int) {
        BRANCH(0),
        LEAF(1),
        OVERFLOW(2),
        META(3),
        DIRTY(4),
        LEAF2(5),
        SUBP(6),
        LOOSE(14),
        KEEP(15)
    }

    /**
     * Database flags
     * http://www.lmdb.tech/doc/group__mdb__dbi__open.html
     */
    enum class Db(val _idx: Int) {
        REVERSEKEY(0),
        DUPSORT(1),
        INTEGERKEY(2),
        DUPFIXED(3),
        INTEGERDUP(4),
        REVERSEDUP(5),
        CREATE(14)
    }

    /**
     * Node flags
     * http://www.lmdb.tech/doc/group__mdb__node.html
     */
    enum class Node(val _idx: Int) {
        BIGDATA(0),
        SUBDATA(1),
        DUPDATA(2)
    }

    companion object {
        fun <T : Enum<T>> fromBuffer(clazz: Class<T>, buffer: ByteBuffer, byteCount: UShort): EnumSet<T> =
            BitSet.valueOf(buffer.slice(buffer.position(), byteCount.toInt())).let { bitset ->
                buffer.positionRelative(byteCount.toInt())
                val constants = clazz.enumConstants
                EnumSet.noneOf(clazz).apply {
                    addAll(IntRange(0, bitset.size()).flatMap {
                        if (bitset[it]) listOf(constants[it]) else emptyList()
                    })
                }
            }
    }
}