@file:Suppress("unused")

package com.growse.lmdb_kt

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.ints.shouldBeExactly
import kotlin.random.Random

class UtilityTests :
    BehaviorSpec({
      val random = Random(1)
      val b1 = ByteArray(50).run(random::nextBytes)
      val b2 = ByteArray(85).run(random::nextBytes)
      Given("Two bytearrays that are the same") {
        When("comparing them") {
          Then("the result is zero") { b1.compareWith(b1) shouldBeExactly 0 }
        }
      }
      Given("Two bytearrays that are different") {
        When("when comparing the larger with the smaller") {
          Then("then the result is -1") { b2.compareWith(b1) shouldBeExactly -1 }
        }
        When("when comparing the larger with the smaller") {
          Then("then the result is 1") { b1.compareWith(b2) shouldBeExactly 1 }
        }
      }
    })
