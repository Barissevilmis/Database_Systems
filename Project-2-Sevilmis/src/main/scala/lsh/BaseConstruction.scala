package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import scala.math.min

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])]) extends Construction {
  /*
  * Initialize LSH data structures here
  * */


  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*

    * This method performs a near-neighbor computation for the data points in rdd against the data points in data.
    * You need to perform the queries by using LSH with min-hash
    * The perturbations needs to be consistent - decided once and randomly for each BaseConstructor object
    * sqlContext: current SQLContext
    * data: data points in (movie_name, [keyword_list]) format to compare against
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */
    val rnd = new scala.util.Random()
    val N : Int = Math.pow(2,30).toInt
    val a : Int = rnd.nextInt(N / 2) + Math.pow(2,4).toInt
    val b : Int = rnd.nextInt(N / 2) + Math.pow(2,4).toInt
    val prime: Int = 96704411


    //val rdd_modified : RDD[(Int, String)] = rdd.map(x =>  (x._1, x._2.map(z => z.hashCode().toString))).map(x => (x._2.map(y=>(a*y.toInt+b)%prime).min, x._1))

    // val data_modified :  RDD[(Int, String)]  = data.map(x => (x._1, x._2.map(z => z.hashCode().toString))).map(x => (x._2.map(y=>(a*y.toInt+b)%prime).min, x._1))

    val rdd_modified  : RDD[(Int, String)]   = rdd.map(x => ( x._2.map(z => (a*z.hashCode()+b)%prime).min, x._1))
    val data_modified :  RDD[(Int, String)]  = data.map(x => ( x._2.map(z => (a*z.hashCode()+b)%prime).min, x._1))
    val res  = rdd_modified.join(data_modified).map(x=> x._2).groupByKey().map(x=>(x._1,x._2.toSet))

    res
  }



}






