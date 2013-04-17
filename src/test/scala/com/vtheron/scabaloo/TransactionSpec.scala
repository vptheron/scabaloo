package com.vtheron.scabaloo

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import java.sql._
import java.net.URL
import scala.Some

class TransactionSpec extends Specification with Mockito {

  import Transaction._

  private val parameters = 42 :: "hello" :: "world" :: Nil

  private def newTransaction(ps: PreparedStatement, query: String): Transaction = {
    val conn = mock[Connection]
    conn.prepareStatement(query) returns ps
    new Transaction(conn)
  }

  private val setToCurrentRow = new collection.mutable.HashMap[ResultSet, Int]

  private def mockResultSet(items: List[(Int, String, Boolean)]): ResultSet = {
    val rs = mock[ResultSet]
    setToCurrentRow += (rs -> 0)

    rs.next() answers (any => {
      val hasNext = setToCurrentRow(rs) < items.length
      setToCurrentRow += (rs -> (setToCurrentRow(rs) + 1))
      hasNext
    })

    rs.getRow answers (any => setToCurrentRow(rs))
    rs.getInt(1) answers (any => items(setToCurrentRow(rs) - 1)._1)
    rs.getString(2) answers (any => items(setToCurrentRow(rs) - 1)._2)
    rs.getBoolean(3) answers (any => items(setToCurrentRow(rs) - 1)._3)
    rs
  }

  "A Transaction instance" should {

    "properly execute a statement" in {
      val ps = mock[PreparedStatement]
      ps.executeUpdate() returns 2

      newTransaction(ps, "updateQuery")
        .statement("updateQuery", parameters: _*) must beEqualTo(2)

      there was one(ps).setInt(1, 42) andThen
        one(ps).setString(2, "hello") andThen
        one(ps).setString(3, "world") andThen
        one(ps).executeUpdate() andThen
        one(ps).close()
    }

    "close the preparedStatement even if the statement fails" in {
      val ps = mock[PreparedStatement]
      ps.executeUpdate() throws new SQLException()

      newTransaction(ps, "updateQuery")
        .statement("updateQuery", parameters: _*) must throwA[SQLException]

      there was one(ps).setInt(1, 42) andThen
        one(ps).setString(2, "hello") andThen
        one(ps).setString(3, "world") andThen
        one(ps).executeUpdate() andThen
        one(ps).close()
    }

    "close the preparedStatement even if the parameters cannot be set in the statement" in {
      val ps = mock[PreparedStatement]
      ps.setString(2, "hello") throws new SQLException()

      newTransaction(ps, "updateQuery").statement("updateQuery", parameters: _*) must throwA[SQLException]

      there was no(ps).executeUpdate()
      there was one(ps).setInt(1, 42) andThen
        one(ps).setString(2, "hello") andThen
        one(ps).close()
    }

    "properly execute a query" in {
      val list = List((1, "hello", true), (2, "world", false), (3, "!", true))
      val rs = mockResultSet(list)
      val ps = mock[PreparedStatement]
      ps.executeQuery() returns rs

      val extractor = (rec: Record) => {
        rec.rowNumber must beEqualTo(setToCurrentRow(rs))
        (rec.intAtIndex(1), rec.stringAtIndex(2), rec.booleanAtIndex(3))
      }

      newTransaction(ps, "selectQuery")
        .query("selectQuery", parameters: _*)(extractor) must beEqualTo(list)

      there was one(ps).setInt(1, 42) andThen
        one(ps).setString(2, "hello") andThen
        one(ps).setString(3, "world") andThen
        one(ps).executeQuery() andThen
        one(rs).close() andThen
        one(ps).close()
    }

  }

  private val index = 1

  "A Transaction object" should {

    "reject a null parameter" in {
      val ps = mock[PreparedStatement]
      setParam(ps, index, null) must throwA(
        new NullPointerException("Element at index " + index + " is null! Use NullType instance instead."))
    }

    "be able to set a BigDecimal" in {
      val bd = BigDecimal(42)
      val ps = mock[PreparedStatement]
      setParam(ps, index, bd)
      there was one(ps).setBigDecimal(index, bd.bigDecimal)
    }

    "be able to set a Boolean" in {
      val boolean = true
      val ps = mock[PreparedStatement]
      setParam(ps, index, boolean)
      there was one(ps).setBoolean(index, boolean)
    }

    "be able to set a Date" in {
      val date = new Date(1234567890L)
      val ps = mock[PreparedStatement]
      setParam(ps, index, date)
      there was one(ps).setDate(index, date)
    }

    "be able to set a Double" in {
      val d = 42.42D
      val ps = mock[PreparedStatement]
      setParam(ps, index, d)
      there was one(ps).setDouble(index, d)
    }

    "be able to set a Float" in {
      val f = 42.42F
      val ps = mock[PreparedStatement]
      setParam(ps, index, f)
      there was one(ps).setFloat(index, f)
    }

    "be able to set a Int" in {
      val i = 42
      val ps = mock[PreparedStatement]
      setParam(ps, index, i)
      there was one(ps).setInt(index, i)
    }

    "be able to set a Long" in {
      val l = 42L
      val ps = mock[PreparedStatement]
      setParam(ps, index, l)
      there was one(ps).setLong(index, l)
    }

    "be able to set a Short" in {
      val s: Short = 42
      val ps = mock[PreparedStatement]
      setParam(ps, index, s)
      there was one(ps).setShort(index, s)
    }

    "be able to set a String" in {
      val s = "hello"
      val ps = mock[PreparedStatement]
      setParam(ps, index, s)
      there was one(ps).setString(index, s)
    }

    "be able to set a Time" in {
      val t = new Time(1234567890L)
      val ps = mock[PreparedStatement]
      setParam(ps, index, t)
      there was one(ps).setTime(index, t)
    }

    "be able to set a Timestamp" in {
      val ts = new Timestamp(1234567890)
      val ps = mock[PreparedStatement]
      setParam(ps, index, ts)
      there was one(ps).setTimestamp(index, ts)
    }

    "be able to set a Some" in {
      val ps = mock[PreparedStatement]
      setParam(ps, index, Some("hello"))
      there was one(ps).setString(index, "hello")
    }

    "be able to set a NullType" in {
      import NullType._

      val ps = mock[PreparedStatement]
      setParam(ps, index, nullInt)
      there was one(ps).setNull(index, nullInt.sqlType)
    }

    "attempt to set a None as null" in {
      val ps = mock[PreparedStatement]
      setParam(ps, index, None)
      there was one(ps).setObject(index, null)
    }

    "attempt to set an unknown object" in {
      val url = new URL("http://www.google.com")
      val ps = mock[PreparedStatement]
      setParam(ps, index, url)
      there was one(ps).setObject(index, url)
    }

  }

}
