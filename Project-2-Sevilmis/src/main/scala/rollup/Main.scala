package rollup

import org.apache.spark.sql.functions._
import java.io._

import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object Main {
  def main(args: Array[String]) {

    val t1 = System.nanoTime
    val spark = SparkSession
      .builder()
      .appName("Project2")
      .master("local[*]")
      .getOrCreate()


    val input = new File(getClass.getResource("/lineorder_small.tbl").getFile).getPath
    val df = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(input)
    //.load("/user/cs422/lineorder_small.tbl")

    val rdd = df.rdd

    var groupingList = List(0, 1, 3)
    //var groupingList = List(0, 1, 3, 5, 7)
    //var groupingList = List(0, 1, 3, 5, 6, 7, 9)
    val rollup = new RollupOperator

    //val res = rollup.rollup_naive(rdd, groupingList, 8, "AVG")
    val res = rollup.rollup(rdd, groupingList, 8, "SUM")


    //res.foreach(x => println(x))
    val duration = (System.nanoTime - t1) / 1e9d


    val correctRes = df.rollup("lo_orderkey", "lo_linenumber", "lo_partkey"/*, "lo_orderdate","lo_shippriority"*/).agg(sum("lo_quantity")).rdd
                                .map(row => (row.toSeq.toList.dropRight(1).filter(x => x != null), row(row.size - 1)))

    println("It took:", duration, " with rdd count: ",rdd.count(), " with result count: ", res.count())
    //println(res.count(),correctRes.count())

    //val a = correctRes.subtractByKey(res)
    //println(a.count())

    //correctRes.foreach(x => println(x))
  }
}