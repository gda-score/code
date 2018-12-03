package examples

import java.io.File
import java.sql.DriverManager

import com.uber.engsec.dp.analysis.histogram.HistogramAnalysis
import com.uber.engsec.dp.rewriting.differential_privacy.{ElasticSensitivityConfig, ElasticSensitivityRewriter, SampleAndAggregateConfig, SampleAndAggregateRewriter}
import com.uber.engsec.dp.schema.Schema
import com.uber.engsec.dp.sql.QueryParser
import com.uber.engsec.dp.util.ElasticSensitivity

import scala.io.Source
import scala.util.parsing.json.JSON

/** A simple example demonstrating query rewriting for differential privacy.
  */
object QueryRewritingExample extends App {
  // Use the table schemas and metadata defined by the test classes
  System.setProperty("schema.config.path", "src/test/resources/schema.yaml")

  //Give database name here from schema
  val database = Schema.getDatabase("raw_banking")

  // variable to store JSON string extracted from the file
  var jsonStr = ""

  // variable to count number of JSON files in the directory
  var fileCount = 0

  // give path where JSON files are created by simpleServer.py
  val path: String = "<path>"

  // privacy budget
  val EPSILON = 0.1
  // delta parameter: use 1/n^2, with n = 100000
  val DELTA = 1 / (math.pow(100000, 2))

  // Helper function to print queries with indentation.
  def printQuery(query: String) = println(s"\n  " + query.replaceAll("\\n", s"\n  ") + "\n")


  val t = new java.util.Timer()
  val task = new java.util.TimerTask {
    def run() = {
      // count the number of files with .json extension in the directory
      val tempFileCount = Option(new File(path).list).map(_.filter(_.endsWith(".json")).size).getOrElse(0)

      // if there is a new file created then get the file name
      if (tempFileCount != fileCount) {
        fileCount = tempFileCount
        var lastFileCreated = new File(path).list()(tempFileCount - 1)

        // append filename to the filepath
        val filename: String = path + lastFileCreated


        // read file to process JSON String
        for (line <- Source.fromFile(filename).getLines) {
          jsonStr = line;
        }


        // extract the query from the JSON
        val resultOption = JSON.parseFull(jsonStr) match {
          case Some(map: Map[String, String]) => map.get("query")
          case _ => None
        }
        val query = resultOption.get

        // create connection to database
        classOf[org.postgresql.Driver]

        // enter appropriate credentials to connect to server
        val con_str = "jdbc:postgresql://db001.gda-score.org:5432/raw_banking?user=<username>&password=<password>"

        val conn = DriverManager.getConnection(con_str)

        def elasticSensitivityExample() = {
          println("*** Elastic sensitivity example ***")

          // Example query given in Uber repository
          /*val query = """
                        |SELECT COUNT(*) FROM orders
                        |JOIN customers ON orders.customer_id = customers.customer_id
                        |WHERE orders.product_id = 1 AND customers.address LIKE '%United States%'"""
            .stripMargin.stripPrefix("\n")*/

          // Print the example query and privacy budget
          val root = QueryParser.parseToRelTree(query, database)
          println("Original query:")
          printQuery(query)
          println(s"> Epsilon: $EPSILON")

          // Compute mechanism parameter values from the query. Note the rewriter does this automatically; here we calculate
          // the values manually so we can print them.
          val elasticSensitivity = ElasticSensitivity.smoothElasticSensitivity(root, database, 0, EPSILON, DELTA)
          println(s"> Elastic sensitivity of this query: $elasticSensitivity")
          println(s"> Required scale of Laplace noise: 2 * $elasticSensitivity / $EPSILON = ${2 * elasticSensitivity / EPSILON}")

          // Rewrite the original query to enforce differential privacy using Elastic Sensitivity.
          println("\nRewritten query:")
          val config = new ElasticSensitivityConfig(EPSILON, DELTA, database)
          val rewrittenQuery = new ElasticSensitivityRewriter(config).run(query)
          printQuery(rewrittenQuery.toSql())
        }

        def sampleAndAggregateExample() = {
          println("*** Sample and aggregate example ***")
          val LAMBDA = 2.0

          // Example query given in Uber repository
          /*val query = """
                        |SELECT AVG(order_cost) FROM orders
                        |WHERE product_id = 1"""
            .stripMargin.stripPrefix("\n")*/

          // Print the example query and privacy budget
          val root = QueryParser.parseToRelTree(query, database)
          println("Original query:")
          printQuery(query)
          println(s"> Epsilon: $EPSILON")

          // Compute mechanism parameter values from the query. Note the rewriter does this automatically; here we calculate
          // the values manually so we can print them.
          val analysisResults = new HistogramAnalysis().run(root, database).colFacts.head
          println(s"> Aggregation function applied: ${analysisResults.outermostAggregation}")
          val tableName = analysisResults.references.head.table
          val approxRowCount = Schema.getTableProperties(database, tableName)("approxRowCount").toLong

          println(s"> Table being queried: $tableName")
          println(s"> Approximate cardinality of table '$tableName': $approxRowCount")
          println(s"> Number of partitions (default heuristic): $approxRowCount ^ 0.4 = ${math.floor(math.pow(approxRowCount, 0.4)).toInt}")
          println(s"> Lambda: $LAMBDA")

          // Rewrite the original query to enforce differential privacy using Sample and Aggregate.
          println("\nRewritten query:")
          val config = new SampleAndAggregateConfig(EPSILON, LAMBDA, database)
          val rewrittenQuery = new SampleAndAggregateRewriter(config).run(query)
          printQuery(rewrittenQuery.toSql())
        }

        elasticSensitivityExample()
        sampleAndAggregateExample()


      }

    }
  }

  // schedule the task to check for new files and execute if new files are found
  t.schedule(task, 100L, 100L)
  task.run()
}