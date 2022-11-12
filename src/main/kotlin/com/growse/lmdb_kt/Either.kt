package com.growse.lmdb_kt

/**
 * An implementation of an algebraic sum type
 *
 * @param A left type
 * @param B right type
 */
sealed class Either<A, B> {
    class Left<A, B>(val left: A) : Either<A, B>()
    class Right<A, B>(val right: B) : Either<A, B>()
}
