package com.vtheron.scabaloo

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.h2.jdbcx.JdbcConnectionPool
import NullType._
import java.sql.{SQLException, Timestamp, Connection}

class DatabaseInt extends Specification with Mockito {


  private def setupTables(connectionProvider: () => Connection) {
    val connection = connectionProvider()
    connection.prepareStatement("CREATE TABLE table1(field1 INTEGER, field2 VARCHAR(255), field3 VARCHAR(255))").executeUpdate()
    connection.prepareStatement("CREATE TABLE table2(field1 INTEGER, field2 BOOLEAN, field3 TIMESTAMP)").executeUpdate()
    connection.close()
    ()
  }

  private def deleteTables(connectionProvider: () => Connection) {
    val connection = connectionProvider()
    connection.prepareStatement("DROP TABLE table1").executeUpdate()
    connection.prepareStatement("DROP TABLE table2").executeUpdate()
    connection.close()
  }

  private def connectionPool(dbName: String): JdbcConnectionPool =
    JdbcConnectionPool.create("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1", "user", "password")

  private def stopDB(connectionPool: JdbcConnectionPool) {
    connectionPool.dispose()
  }

  "A Database" should {

    "insert some values and fetch them" in {
      val pool = connectionPool("test1")
      val provider = () => pool.getConnection
      setupTables(provider)

      val db = new Database(provider)

      val insertCount = db.statement("INSERT INTO table1 VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
        1, "hello", "english", 2, "bonjour", "francais", 3, "goddag", "svenska")

      insertCount must beEqualTo(3)

      val languages = db.query("SELECT * FROM table1 WHERE field2 = ? OR field3 = ?", "bonjour", "svenska") {
        r => (r.intAtIndex(1), r.stringInColumn("field3"))
      }

      languages must beEqualTo((2, "francais") ::(3, "svenska") :: Nil)
      deleteTables(provider)
      stopDB(pool)
    }

    "insert some values and fetch options" in {
      val pool = connectionPool("test2")
      val provider = () => pool.getConnection
      setupTables(provider)

      val db = new Database(provider)

      val insertCount = db.statement("INSERT INTO table1 VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
        1, "hello", "english", 2, None, "francais", 3, nullString, "svenska")

      insertCount must beEqualTo(3)

      val languages = db.query("SELECT * FROM table1") {
        r => (r.intAtIndex(1), r.stringInColumn_?("field2"))
      }

      languages must beEqualTo((1, Some("hello")) ::(2, None) ::(3, None) :: Nil)
      deleteTables(provider)
      stopDB(pool)
    }

    "execute a transaction" in {
      val pool = connectionPool("test3")
      val provider = () => pool.getConnection
      setupTables(provider)

      val ts = new Timestamp(1234567890L)

      val db = new Database(provider)

      val res = db.inTransaction(t => {
        t.statement("INSERT INTO table1 VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
          1, "hello", "english", 2, None, "francais", 3, nullString, "svenska") must beEqualTo(3)
        t.statement("INSERT INTO table2 VALUES (?, ?, ?), (?, ?, ?)",
          1, true, ts, 2, false, nullTimestamp) must beEqualTo(2)

        t.query("SELECT t1.field1, t2.field2, t2.field3 FROM table1 as t1, table2 as t2 WHERE t1.field1 = t2.field1")(r => {
          (r.intInColumn("field1"), r.booleanAtIndex(2), r.timestampAtIndex_?(3))
        })
      })

      res must beEqualTo((1, true, Some(ts)) ::(2, false, None) :: Nil)

      deleteTables(provider)
      stopDB(pool)
    }

    "rollback a transaction if something goes wrong" in {
      val pool = connectionPool("test4")
      val provider = () => pool.getConnection
      setupTables(provider)

      val db = new Database(provider)

      db.inTransaction(t => {
        t.statement("INSERT INTO table1 VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
          1, "hello", "english", 2, None, "francais", 3, nullString, "svenska") must beEqualTo(3)

        t.query("SELECT * FROM table1")(r => r.intAtIndex(1)) must beEqualTo(1 :: 2 :: 3 :: Nil)

        t.statement("INSERT INTO table2 VALUES (?, ?)", 1, "wait, this is not a varchar field! NOOO!")
      }) must throwA[SQLException]

      db.query("SELECT * FROM table1")(r => r.intAtIndex(1)) must beEqualTo(Nil)
      db.query("SELECT * FROM table2")(r => r.intAtIndex(1)) must beEqualTo(Nil)

      deleteTables(provider)
      stopDB(pool)
    }

    "not let a user insert null as a value" in {
      val pool = connectionPool("test5")
      val provider = () => pool.getConnection
      setupTables(provider)

      val db = new Database(provider)

      db.statement("INSERT INTO table1 VALUES (?, ?, ?), (?, ?, ?)",
          1, "hello", "english", 2, null, "francais") must throwA(
        new NullPointerException("Element at index 5 is null! Use NullType instance instead."))

      deleteTables(provider)
      stopDB(pool)
    }

    "not let a user retrieve null as a value" in {
      val pool = connectionPool("test6")
      val provider = () => pool.getConnection
      setupTables(provider)

      val db = new Database(provider)

      db.statement("INSERT INTO table1 VALUES (?, ?, ?), (?, ?, ?)",
          1, "hello", "english", 2, nullString, "francais") must beEqualTo(2)

      db.query("SELECT * FROM table1")(r => r.stringAtIndex(2)) must throwA(
        new NullPointerException("Element at index 2 was null."))

      deleteTables(provider)
      stopDB(pool)
    }

  }

}
