package lsh

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, broadcast}

import scala.math.min

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])]) extends Construction with Serializable {
  /*
  * Initialize LSH data structures here
  * You need to broadcast the data structures to all executors and use them locally
  * */
  val rnd = new scala.util.Random()
  val N : Int = Math.pow(2,24).toInt
  val a : Int = rnd.nextInt(N / 2) + Math.pow(2,10).toInt
  val b : Int = rnd.nextInt(N / 2) + Math.pow(2,10).toInt
  val prime: Int = 46704331

  val data_modified :  RDD[(Int, Set[String])]  = data.map(x => ( x._2.map(z => (a*z.hashCode()+b)%prime).min, x._1)).groupByKey().map(x=>(x._1,x._2.toSet))

  val rddMap: Map[Int, Set[String]] = data_modified.collect().toMap
  val bc: Broadcast[Map[Int, Set[String]]] = SparkContext.getOrCreate().broadcast(rddMap)


  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*
    * This method performs a near-neighbor computation for the data points in rdd against the data points in data.
    * You need to perform the queries by using LSH with min-hash.
    * The perturbations needs to be consistent - decided once and randomly for each BaseConstructor object
    * sqlContext: current SQLContext
    * data: data points in (movie_name, [keyword_list]) format to compare against
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */


    val res: RDD[(String, Set[String])] = rdd.map(x => ( x._2.map(z => (a*z.hashCode()+b)%prime).min, x._1))
      .map(x=>(x._2, bc.value.getOrElse(x._1, Set[String]())))

    res
  }
}
