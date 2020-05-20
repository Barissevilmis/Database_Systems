package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SQLContext, Row}


import java.io._

object Main {
  def main(args: Array[String]) {

    val t1 = System.nanoTime

    val spark = SparkSession
      .builder()
      .appName("Project2")
    //  .master("local[*]")
      .getOrCreate()

    val attrIndex1 = 1
    val attrIndex2 = 1


    //val rdd1 = loadRDD(spark.sqlContext, "/dat1_4.csv")
    //val rdd2 = loadRDD(spark.sqlContext, "/dat2_4.csv")
    val rdd1 = loadRDD(spark.sqlContext, "/user/cs422/taxA4K.csv")
    val rdd2 = loadRDD(spark.sqlContext, "/user/cs422/taxB4K.csv")
    val thetaJoin = new ThetaJoin(400)
    val res = thetaJoin.ineq_join(rdd1, rdd2, attrIndex1, attrIndex2, "<")
    //res.sortBy(x => (x._1, x._2)).collect().toList.foreach(x => println("pepe ",x))
    res.count()
    //val rdd1 = loadRDD(spark.sqlContext, "/user/cs422/taxA50K.csv")
    //val rdd2 = loadRDD(spark.sqlContext, "/user/cs422/taxB50K.csv")
    //val rdd1 = loadRDD(spark.sqlContext, "/taxA4K.csv")
    //val rdd2 = loadRDD(spark.sqlContext, "/taxB4K.csv")
    //1 4
    //val thetaJoin = new ThetaJoin(16)
    //val res = thetaJoin.ineq_join(rdd1, rdd2, attrIndex1, attrIndex2, "<")

    val duration = (System.nanoTime - t1) / 1e9d
    println("It took:", duration)

    //res.sortBy(x => (x._1, x._2)).collect().toList.foreach(x => println("pepe ",x))


    // use the cartesian product to verify correctness of your result
   /* val cartesianRes = rdd1.cartesian(rdd2)
                         .filter(x => x._1(attrIndex1).asInstanceOf[Int] < x._2(attrIndex2).asInstanceOf[Int])
                         .map(x => (x._1(attrIndex1).asInstanceOf[Int], x._2(attrIndex2).asInstanceOf[Int]))
    //cartesianRes.foreach(x => println(x))
    assert(res.sortBy(x => (x._1, x._2)).collect().toList.equals(cartesianRes.sortBy(x => (x._1, x._2)).collect().toList))*/

  }

  def loadRDD(sqlContext: SQLContext, file: String): RDD[Row] = {
   // val input = new File(getClass.getResource(file).getFile).getPath

    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(file).rdd
  }
}
