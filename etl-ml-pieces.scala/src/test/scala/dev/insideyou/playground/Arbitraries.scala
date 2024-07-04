package dev.insideyou
package playground

//import derevo.scalacheck
import derevo.scalacheck.arbitrary
//import org.scalacheck
import org.scalacheck.Arbitrary

/*
//      "derevo",
//      "derevo.scalacheck",
//      "org.scalacheck",
//      "org.scalacheck.Prop",

* */

// Put all of your arbitraries in here
// For data structures defined in your tests (like Person actually)
// you can also use the @derive(arbitrary) annotation
@SuppressWarnings(Array("org.wartremover.warts.All"))
trait Arbitraries {
  implicit protected val arbitraryPerson: Arbitrary[Person] =
    arbitrary.instance
}
