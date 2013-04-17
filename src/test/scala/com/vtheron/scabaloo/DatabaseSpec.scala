package com.vtheron.scabaloo

import org.specs2.mutable.Specification
import java.sql.Connection
import org.specs2.mock.Mockito

class DatabaseSpec extends Specification with Mockito {

  "A Database" should {

    "be able to start and commit a transaction" in {
      val connection = mock[Connection]

      val transaction = (t: Transaction) => {
        t.connection must beEqualTo(connection)
        "hello world"
      }

      new Database(() => connection).inTransaction(transaction) must beEqualTo("hello world")

      there was one(connection).setAutoCommit(false) andThen
        one(connection).commit() andThen
        one(connection).setAutoCommit(true) andThen
        one(connection).close()
    }

    "rollback a transaction if an exception is thrown" in {
      val evilException = new IllegalStateException("Don't feel like it.")
      val connection = mock[Connection]

      val transaction: Transaction => AnyRef = t => {
        t.connection must beEqualTo(connection)
        throw evilException
      }

      new Database(() => connection).inTransaction(transaction) must throwA(evilException)

      there was one(connection).setAutoCommit(false) andThen
        one(connection).rollback() andThen
        one(connection).setAutoCommit(true) andThen
        one(connection).close()
    }

  }

}
