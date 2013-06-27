# Scabaloo

*Scabaloo* is a thin wrapper around the classic JDBC API. This is *not* an ORM, there is no mapping between your code and your database. You still have to write raw SQL for your queries, what you don't have to do is deal with `ResultSets`, `PreparedStatements`, etc.

This is a very minimalistic project. The source code is very small, it comes with almost no dependencies (except for the logging framework, slf4j). Do not forget to add the appropriate JDBC driver to your project as a dependency.

## Let's get started

The main object is `Database`, the interface you use to talk to your actual DB (i.e. execute queries and receive results).
To create a `Database` you need a function that takes no arguments and returns a `Connection` instance.

    val connectionProvider: () => Connection = ...
    val database = new Database(connectionProvider)

It is up to you to decide how the connection provider function is implemented. Since, the most common usage is probably to use a `DataSource`, `Database` provides a constructor for that:

    val dataSource: DataSource = ...
    val database = Database(dataSource)

Note that the following code will probably lead to failures:

    val connection: Connection = ...
    val database = new Database( () => connection )

Here we create one `Connection` instance that we use to create a function passed to our new `Database`. The problem happens when we execute a query, the `Database` will execute the function to get a `Connection` instance, run the query and *close* the `Connection`. Therefor, the next query will fail since the `Connection` is no longer valid.

## Querying the database

    val countries = database.query("SELECT * FROM countries WHERE capital = ? OR language = ?", "Paris", "FR") {
                  record => Country(record.intAtIndex(1), record.stringInColumn("name"))
                }

    database.statement("INSERT INTO countries VALUES (?, ?, ?, ?)", 42, "Germany", "Berlin", Some("German"))

    database.inTransaction(transaction => {
        transaction.query("SELECT ...", mapper)
        transaction.statement("INSERT INTO ...", ...)
        transaction.statement("UPDATE ...", ...)
    })

Note how you can use `Some("German")` as a valid value for a query parameter. *Scabaloo* will extract the `String` and use it as a value. So, can you use `None`? Well, you can.

        database.statement("INSERT INTO countries VALUES (?, ?, ?, ?)", 42, "Germany", "Berlin", None)

The statement above will insert a new record in the table `countries` and set the fourth field to `NULL`. Great, but using 'None' means that *Scabaloo* has no idea what the actual type of this parameter is. It will try its best to set it to `null` but this may fail with some JDBC drivers.

## Dealing with null

Null is bad. Very bad. And if you try to use it *Scabaloo* will punish you.

Scabaloo will *never* allow you to use null, either as query parameter value or as result set field. So, how do you represent null values ?

    import com.vtheron.scabaloo.NullType._
    database.statement("INSERT INTO countries VALUES (?,?,?,?)", 42, "Germany", nullString, "German")

The `NullType` object provides typed null values. As explained before, you can choose to use `None` for null, but using a typed null value insures that *Scabaloo* knows the actual type of the parameter and can properly use the JDBC API.

How about retrieving null values?

    val nameCapitalAndLanguage: (String, Option[String], Option[String]) =
        database.query("SELECT * FROM countries") {
            record => (
                record.stringAtIndex(2),
                record.stringInColumn_?("capital"),
                record.stringAtIndex_?(4)
                )
        }

## Supported types

*Scabaloo* explicitly supports the following type:
* scala.math.BigDecimal
* scala.Boolean
* java.sql.Date
* scala.Double
* scala.Float
* scala.Int
* scala.Long
* scala.Short
* scala.String
* java.sql.Time
* java.sql.Timestamp

Additional support is added for `Some[A]`, `None` and the typed null values (see previous part).

If you try to use an unsupported type as query parameter, *Scabaloo* will still try to properly set the value using `statement.setObject(index, element)`. Depending on your JDBC driver and the actual type of element this operation may fail or succeed.

## Good to know

*Scabaloo* is a work in progress and the API is subject to change.

