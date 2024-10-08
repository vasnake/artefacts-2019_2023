/** Created by vasnake@gmail.com on 2024-08-13
  */
package com.github.vasnake.core.text

//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._
//import org.scalactic.Equality

// happy path

class StringToolboxTest extends AnyFlatSpec with should.Matchers {
  import StringToolbox._

  // custom separators
  implicit val sep: Separators = Separators(";", Some(Separators("=")))

  it should "split string to array of non-empty trimmed strings" in {
    assert("foo; bar; baz; bara bas".splitTrim === Array("foo", "bar", "baz", "bara bas"))
  }

  it should "split string to list of non-empty trimmed strings" in {
    assert("foo; bar; baz; bara bas".s2list === Seq("foo", "bar", "baz", "bara bas"))
  }

  it should "parse string to Map[String, String]" in {
    assert("foo = bar; baz = bara bas".parseMap === Map("foo" -> "bar", "baz" -> "bara bas"))
  }
}

// corner cases

class StringToolboxTestSplitTrim extends AnyFlatSpec with should.Matchers {
  import StringToolbox._
  import DefaultSeparators._

  it should "produce empty array" in {
    val expected = Array.empty[String]

    assert("".splitTrim === expected)
    assert(",,,,,,, ,,,, , , , ".splitTrim === expected)
    assert(" , \t , \n \n \t ".splitTrim === expected)
  }

  it should "produce size 1 array" in {
    assert(".".splitTrim === Array("."))
    assert(" \t \n . \n \t ".splitTrim === Array("."))

    val expected = Array("foo")
    val data =
      """
        |foo,
        |,foo,
        |  foo  ,
        | , foo
        |  , foo ,
        | ,,, foo ,,
      """.stripMargin

    for (line <- data.split('\n') if line.trim.length > 0) {
      // println(s"check line: `$line`")
      assert(line.splitTrim === expected)
      assert(s" $line ".splitTrim === expected)
      assert(s" , \t \n $line \t , \n ".splitTrim === expected)
    }
  }

  it should "process complex text" in {
    val text =
      """
        | Mary,
        | had a little,
        | lamb,
        | ,.
      """.stripMargin
    assert(text.splitTrim === Array("Mary", "had a little", "lamb", "."))
  }
}

class StringToolboxTestToList extends AnyFlatSpec with should.Matchers {
  import StringToolbox.RichString
  import StringToolbox.DefaultSeparators.commaColon

  it should "produce empty list" in {
    val empty = Seq.empty[String]

    assert("".s2list === empty)
    assert(",,,,,,, ,,,, , , , ".s2list === empty)
    assert(" , \t , \n \n \t ".s2list === empty)
  }

  it should "produce size 1 list" in {
    assert(".".s2list === Seq("."))
    assert(" \t \n . \n \t ".s2list === Seq("."))

    val expected = Seq("foo")
    val data =
      """
        |foo,
        |,foo,
        |  foo  ,
        | , foo
        |  , foo ,
        | ,,, foo ,,
      """.stripMargin

    for (line <- data.split('\n') if line.trim.length > 0) {
      // println(s"check line: `$line`")
      assert(line.s2list === expected)
      assert(s" $line ".s2list === expected)
      assert(s" , \t \n $line \t , \n ".s2list === expected)
    }
  }

  it should "process complex text" in {
    val text =
      """
        | Mary,
        | had a little,
        | lamb,
        | .
      """.stripMargin
    assert(text.s2list === Seq("Mary", "had a little", "lamb", "."))
  }
}

class StringToolboxTestToMap extends AnyFlatSpec with should.Matchers {
  import StringToolbox.RichString
  import StringToolbox.DefaultSeparators.commaColon

  it should "produce empty map" in {
    val expected = Map.empty[String, String]
    val data =
      """
        |:                  , empty key or value
        |:,:,:,:, ...
        |foo
        |foo, bar
        |foo ,, , , ,, bar
        |foo : bar : baz    , too many parts -- only two make a kv pair
        |foo:bar:baz, noval , too many parts and no value for key
      """.stripMargin

    for (line <- data.split('\n')) {
      // println(s"check line: `$line`")
      assert(line.parseMap === expected)
      assert(s" $line ".parseMap === expected)
      assert(s" \t \n $line \t \n ".parseMap === expected)
    }
  }

  it should "produce size 1 map" in {
    val expected = Map("foo" -> "bar")
    val data =
      """
        |foo:bar
        |foo : bar
        |foo: bar, baz              , no value for 'baz'
        |bar: baz: foo, foo: bar    , too many parts in 'bar...'
        |foo:bar, baz, nay,,, : , : , no values and no keys after firs pair
      """.stripMargin

    for (line <- data.split('\n') if line.trim.length > 0) {
      // println(s"check line: `$line`")
      assert(line.parseMap === expected)
      assert(s" $line ".parseMap === expected)
      assert(s" \t \n $line \t \n ".parseMap === expected)
    }
  }

  it should "save last version of key value" in {
    assert("foo: bar, foo: baz".parseMap === Map("foo" -> "baz"))
  }
}

class StringToolboxTestExtractNumber extends AnyFlatSpec with should.Matchers {
  import StringToolbox._
  implicit val sep: Separators = Separators(" ")

  it should "parse number from a string" in {
    val data =
      """
        |0: 3.33
        |0: 3.33 foo
        |1: foo 3.33
        |1: foo 3.33 bar
        |2: foo bar 3.33
        |2: foo bar 3.33 baz
      """.stripMargin.split("\n").map(_.trim).filter(_.nonEmpty)

    for {
      line <- data
      Array(pos, inp) = line.splitTrim(Separators(": "))
    } {
      assert(inp.extractNumber(pos.toInt) === Some(3.33))
      assert(s"  $inp  ".extractNumber(pos.toInt) === Some(3.33))
      assert(s" oops $inp ".extractNumber(pos.toInt) === None)
    }

    assert("".extractNumber(0) === None)
    assert(" foo ".extractNumber(0) === None)
    assert(" foo bar ".extractNumber(1) === None)
    assert(" 1 2 3 ".extractNumber(3) === None)
  }
}
