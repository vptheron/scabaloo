package com.vtheron.scabaloo

import java.sql.{Date, Time, Timestamp, ResultSet}
import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import java.net.URL

class RecordSpec extends Specification with Mockito {

  private final val index = 42
  private final val elementAtIndexWasNull = new NullPointerException("Element at index " + index + " was null.")
  private final val column = "myField"
  private final val elementInColumnWasNull = new NullPointerException("Element in column " + column + " was null.")

  private def record(rs: ResultSet): Record = new Record(rs, 24)

  "A Record" should {

    "be able to handle int type" in {
      {
        val value = 1
        val rs = mock[ResultSet]
        rs.getInt(index) returns value
        rs.getInt(column) returns value
        rs.wasNull returns false

        record(rs).intAtIndex(index) must beEqualTo(value)
        there was one(rs).getInt(index)
        record(rs).intAtIndex_?(index) must beSome(value)
        there was two(rs).getInt(index)
        record(rs).intInColumn(column) must beEqualTo(value)
        there was one(rs).getInt(column)
        record(rs).intInColumn_?(column) must beSome(value)
        there was two(rs).getInt(column)
      }
      {
        val emptyRs = mock[ResultSet]
        emptyRs.wasNull returns true

        record(emptyRs).intAtIndex(index) must throwA(elementAtIndexWasNull)
        there was one(emptyRs).getInt(index)
        record(emptyRs).intAtIndex_?(index) must beNone
        there was two(emptyRs).getInt(index)
        record(emptyRs).intInColumn(column) must throwA(elementInColumnWasNull)
        there was one(emptyRs).getInt(column)
        record(emptyRs).intInColumn_?(column) must beNone
        there was two(emptyRs).getInt(column)
      }
    }

    "be able to handle String type" in {
      {
        val value = "hello"
        val rs = mock[ResultSet]
        rs.getString(index) returns value
        rs.getString(column) returns value
        rs.wasNull returns false

        record(rs).stringAtIndex(index) must beEqualTo(value)
        there was one(rs).getString(index)
        record(rs).stringAtIndex_?(index) must beSome(value)
        there was two(rs).getString(index)
        record(rs).stringInColumn(column) must beEqualTo(value)
        there was one(rs).getString(column)
        record(rs).stringInColumn_?(column) must beSome(value)
        there was two(rs).getString(column)
      }
      {
        val emptyRs = mock[ResultSet]
        emptyRs.wasNull returns true

        record(emptyRs).stringAtIndex(index) must throwA(elementAtIndexWasNull)
        there was one(emptyRs).getString(index)
        record(emptyRs).stringAtIndex_?(index) must beNone
        there was two(emptyRs).getString(index)
        record(emptyRs).stringInColumn(column) must throwA(elementInColumnWasNull)
        there was one(emptyRs).getString(column)
        record(emptyRs).stringInColumn_?(column) must beNone
        there was two(emptyRs).getString(column)
      }
    }

    "be able to handle long type" in {
      {
        val value = 1L
        val rs = mock[ResultSet]
        rs.getLong(index) returns value
        rs.getLong(column) returns value
        rs.wasNull returns false

        record(rs).longAtIndex(index) must beEqualTo(value)
        there was one(rs).getLong(index)
        record(rs).longAtIndex_?(index) must beSome(value)
        there was two(rs).getLong(index)
        record(rs).longInColumn(column) must beEqualTo(value)
        there was one(rs).getLong(column)
        record(rs).longInColumn_?(column) must beSome(value)
        there was two(rs).getLong(column)
      }
      {
        val emptyRs = mock[ResultSet]
        emptyRs.wasNull returns true

        record(emptyRs).longAtIndex(index) must throwA(elementAtIndexWasNull)
        there was one(emptyRs).getLong(index)
        record(emptyRs).longAtIndex_?(index) must beNone
        there was two(emptyRs).getLong(index)
        record(emptyRs).longInColumn(column) must throwA(elementInColumnWasNull)
        there was one(emptyRs).getLong(column)
        record(emptyRs).longInColumn_?(column) must beNone
        there was two(emptyRs).getLong(column)
      }
    }

    "be able to handle double type" in {
      {
        val value = 1.5D
        val rs = mock[ResultSet]
        rs.getDouble(index) returns value
        rs.getDouble(column) returns value
        rs.wasNull returns false

        record(rs).doubleAtIndex(index) must beEqualTo(value)
        there was one(rs).getDouble(index)
        record(rs).doubleAtIndex_?(index) must beSome(value)
        there was two(rs).getDouble(index)
        record(rs).doubleInColumn(column) must beEqualTo(value)
        there was one(rs).getDouble(column)
        record(rs).doubleInColumn_?(column) must beSome(value)
        there was two(rs).getDouble(column)
      }
      {
        val emptyRs = mock[ResultSet]
        emptyRs.wasNull returns true

        record(emptyRs).doubleAtIndex(index) must throwA(elementAtIndexWasNull)
        there was one(emptyRs).getDouble(index)
        record(emptyRs).doubleAtIndex_?(index) must beNone
        there was two(emptyRs).getDouble(index)
        record(emptyRs).doubleInColumn(column) must throwA(elementInColumnWasNull)
        there was one(emptyRs).getDouble(column)
        record(emptyRs).doubleInColumn_?(column) must beNone
        there was two(emptyRs).getDouble(column)
      }
    }

    "be able to handle float type" in {
      {
        val value = 1.5F
        val rs = mock[ResultSet]
        rs.getFloat(index) returns value
        rs.getFloat(column) returns value
        rs.wasNull returns false

        record(rs).floatAtIndex(index) must beEqualTo(value)
        there was one(rs).getFloat(index)
        record(rs).floatAtIndex_?(index) must beSome(value)
        there was two(rs).getFloat(index)
        record(rs).floatInColumn(column) must beEqualTo(value)
        there was one(rs).getFloat(column)
        record(rs).floatInColumn_?(column) must beSome(value)
        there was two(rs).getFloat(column)
      }
      {
        val emptyRs = mock[ResultSet]
        emptyRs.wasNull returns true

        record(emptyRs).floatAtIndex(index) must throwA(elementAtIndexWasNull)
        there was one(emptyRs).getFloat(index)
        record(emptyRs).floatAtIndex_?(index) must beNone
        there was two(emptyRs).getFloat(index)
        record(emptyRs).floatInColumn(column) must throwA(elementInColumnWasNull)
        there was one(emptyRs).getFloat(column)
        record(emptyRs).floatInColumn_?(column) must beNone
        there was two(emptyRs).getFloat(column)
      }
    }

    "be able to handle short type" in {
      {
        val value = 1.toShort
        val rs = mock[ResultSet]
        rs.getShort(index) returns value
        rs.getShort(column) returns value
        rs.wasNull returns false

        record(rs).shortAtIndex(index) must beEqualTo(value)
        there was one(rs).getShort(index)
        record(rs).shortAtIndex_?(index) must beSome(value)
        there was two(rs).getShort(index)
        record(rs).shortInColumn(column) must beEqualTo(value)
        there was one(rs).getShort(column)
        record(rs).shortInColumn_?(column) must beSome(value)
        there was two(rs).getShort(column)
      }
      {
        val emptyRs = mock[ResultSet]
        emptyRs.wasNull returns true

        record(emptyRs).shortAtIndex(index) must throwA(elementAtIndexWasNull)
        there was one(emptyRs).getShort(index)
        record(emptyRs).shortAtIndex_?(index) must beNone
        there was two(emptyRs).getShort(index)
        record(emptyRs).shortInColumn(column) must throwA(elementInColumnWasNull)
        there was one(emptyRs).getShort(column)
        record(emptyRs).shortInColumn_?(column) must beNone
        there was two(emptyRs).getShort(column)
      }
    }

    "be able to handle boolean type" in {
      {
        val value = true
        val rs = mock[ResultSet]
        rs.getBoolean(index) returns value
        rs.getBoolean(column) returns value
        rs.wasNull returns false

        record(rs).booleanAtIndex(index) must beEqualTo(value)
        there was one(rs).getBoolean(index)
        record(rs).booleanAtIndex_?(index) must beSome(value)
        there was two(rs).getBoolean(index)
        record(rs).booleanInColumn(column) must beEqualTo(value)
        there was one(rs).getBoolean(column)
        record(rs).booleanInColumn_?(column) must beSome(value)
        there was two(rs).getBoolean(column)
      }
      {
        val emptyRs = mock[ResultSet]
        emptyRs.wasNull returns true

        record(emptyRs).booleanAtIndex(index) must throwA(elementAtIndexWasNull)
        there was one(emptyRs).getBoolean(index)
        record(emptyRs).booleanAtIndex_?(index) must beNone
        there was two(emptyRs).getBoolean(index)
        record(emptyRs).booleanInColumn(column) must throwA(elementInColumnWasNull)
        there was one(emptyRs).getBoolean(column)
        record(emptyRs).booleanInColumn_?(column) must beNone
        there was two(emptyRs).getBoolean(column)
      }
    }

    "be able to handle BigDecimal type" in {
      {
        val value = BigDecimal(42)
        val rs = mock[ResultSet]
        rs.getBigDecimal(index) returns value.bigDecimal
        rs.getBigDecimal(column) returns value.bigDecimal
        rs.wasNull returns false

        record(rs).bigDecimalAtIndex(index) must beEqualTo(value)
        there was one(rs).getBigDecimal(index)
        record(rs).bigDecimalAtIndex_?(index) must beSome(value)
        there was two(rs).getBigDecimal(index)
        record(rs).bigDecimalInColumn(column) must beEqualTo(value)
        there was one(rs).getBigDecimal(column)
        record(rs).bigDecimalInColumn_?(column) must beSome(value)
        there was two(rs).getBigDecimal(column)
      }
      {
        val emptyRs = mock[ResultSet]
        emptyRs.wasNull returns true

        record(emptyRs).bigDecimalAtIndex(index) must throwA(elementAtIndexWasNull)
        there was one(emptyRs).getBigDecimal(index)
        record(emptyRs).bigDecimalAtIndex_?(index) must beNone
        there was two(emptyRs).getBigDecimal(index)
        record(emptyRs).bigDecimalInColumn(column) must throwA(elementInColumnWasNull)
        there was one(emptyRs).getBigDecimal(column)
        record(emptyRs).bigDecimalInColumn_?(column) must beNone
        there was two(emptyRs).getBigDecimal(column)
      }
    }

    "be able to handle Timestamp type" in {
      {
        val value = new Timestamp(1234567890L)
        val rs = mock[ResultSet]
        rs.getTimestamp(index) returns value
        rs.getTimestamp(column) returns value
        rs.wasNull returns false

        record(rs).timestampAtIndex(index) must beEqualTo(value)
        there was one(rs).getTimestamp(index)
        record(rs).timestampAtIndex_?(index) must beSome(value)
        there was two(rs).getTimestamp(index)
        record(rs).timestampInColumn(column) must beEqualTo(value)
        there was one(rs).getTimestamp(column)
        record(rs).timestampInColumn_?(column) must beSome(value)
        there was two(rs).getTimestamp(column)
      }
      {
        val emptyRs = mock[ResultSet]
        emptyRs.wasNull returns true

        record(emptyRs).timestampAtIndex(index) must throwA(elementAtIndexWasNull)
        there was one(emptyRs).getTimestamp(index)
        record(emptyRs).timestampAtIndex_?(index) must beNone
        there was two(emptyRs).getTimestamp(index)
        record(emptyRs).timestampInColumn(column) must throwA(elementInColumnWasNull)
        there was one(emptyRs).getTimestamp(column)
        record(emptyRs).timestampInColumn_?(column) must beNone
        there was two(emptyRs).getTimestamp(column)
      }
    }

    "be able to handle Time type" in {
      {
        val value = new Time(1234567890L)
        val rs = mock[ResultSet]
        rs.getTime(index) returns value
        rs.getTime(column) returns value
        rs.wasNull returns false

        record(rs).timeAtIndex(index) must beEqualTo(value)
        there was one(rs).getTime(index)
        record(rs).timeAtIndex_?(index) must beSome(value)
        there was two(rs).getTime(index)
        record(rs).timeInColumn(column) must beEqualTo(value)
        there was one(rs).getTime(column)
        record(rs).timeInColumn_?(column) must beSome(value)
        there was two(rs).getTime(column)
      }
      {
        val emptyRs = mock[ResultSet]
        emptyRs.wasNull returns true

        record(emptyRs).timeAtIndex(index) must throwA(elementAtIndexWasNull)
        there was one(emptyRs).getTime(index)
        record(emptyRs).timeAtIndex_?(index) must beNone
        there was two(emptyRs).getTime(index)
        record(emptyRs).timeInColumn(column) must throwA(elementInColumnWasNull)
        there was one(emptyRs).getTime(column)
        record(emptyRs).timeInColumn_?(column) must beNone
        there was two(emptyRs).getTime(column)
      }
    }

    "be able to handle Date type" in {
      {
        val value = new Date(1234567890L)
        val rs = mock[ResultSet]
        rs.getDate(index) returns value
        rs.getDate(column) returns value
        rs.wasNull returns false

        record(rs).dateAtIndex(index) must beEqualTo(value)
        there was one(rs).getDate(index)
        record(rs).dateAtIndex_?(index) must beSome(value)
        there was two(rs).getDate(index)
        record(rs).dateInColumn(column) must beEqualTo(value)
        there was one(rs).getDate(column)
        record(rs).dateInColumn_?(column) must beSome(value)
        there was two(rs).getDate(column)
      }
      {
        val emptyRs = mock[ResultSet]
        emptyRs.wasNull returns true

        record(emptyRs).dateAtIndex(index) must throwA(elementAtIndexWasNull)
        there was one(emptyRs).getDate(index)
        record(emptyRs).dateAtIndex_?(index) must beNone
        there was two(emptyRs).getDate(index)
        record(emptyRs).dateInColumn(column) must throwA(elementInColumnWasNull)
        there was one(emptyRs).getDate(column)
        record(emptyRs).dateInColumn_?(column) must beNone
        there was two(emptyRs).getDate(column)
      }
    }

    "be able to handle generic object type" in {
      {
        val value = new URL("http://www.google.com")
        val rs = mock[ResultSet]
        rs.getObject(index) returns value
        rs.getObject(column) returns value
        rs.wasNull returns false

        record(rs).objectAtIndex(index) must beEqualTo(value)
        there was one(rs).getObject(index)
        record(rs).objectAtIndex_?(index) must beSome(value)
        there was two(rs).getObject(index)
        record(rs).objectInColumn(column) must beEqualTo(value)
        there was one(rs).getObject(column)
        record(rs).objectInColumn_?(column) must beSome(value)
        there was two(rs).getObject(column)
      }
      {
        val emptyRs = mock[ResultSet]
        emptyRs.wasNull returns true

        record(emptyRs).objectAtIndex(index) must throwA(elementAtIndexWasNull)
        there was one(emptyRs).getObject(index)
        record(emptyRs).objectAtIndex_?(index) must beNone
        there was two(emptyRs).getObject(index)
        record(emptyRs).objectInColumn(column) must throwA(elementInColumnWasNull)
        there was one(emptyRs).getObject(column)
        record(emptyRs).objectInColumn_?(column) must beNone
        there was two(emptyRs).getObject(column)
      }
    }

  }

}
