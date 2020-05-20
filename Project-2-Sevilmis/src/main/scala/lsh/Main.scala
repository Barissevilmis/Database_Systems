package lsh

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object Main {
  def recall(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    /*
    * Compute the recall for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average recall
    * */



    val joined = lsh_truth.join(ground_truth)
    val temp = joined.map(x=>(x._2._1.intersect(x._2._2).size, x._2._2.diff(x._2._1).size))
    val tp = temp.map(_._1).reduce(_ + _)
    val fn = temp.map(_._2).reduce(_ + _)

    print("tp", tp)
    print("fn", fn)
    tp/(tp+fn).toDouble

  }

  def precision(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    /*
    * Compute the precision for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average precision
    * */
    val ground_uncompressed : RDD[(String,String)] = ground_truth.map(x=>(x._2.map(y=>(x._1,y))))
      .flatMap(x => x)

    val lsh_uncompressed : RDD[(String,String)] = lsh_truth.map(x=>(x._2.map(y=>(x._1,y))))
      .flatMap(x => x)

    val tp : Double = ground_uncompressed.intersection(lsh_uncompressed).count().toDouble
    val fp : Double = lsh_uncompressed.subtract(ground_uncompressed).count().toDouble

    tp/(tp+fp)
  }

  def query1 (sc : SparkContext, sqlContext : SQLContext) : Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath


    val rdd_corpus = sc
      .textFile(corpus_file)
    //.textFile("/user/cs422/lsh-corpus-large.csv")
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-1.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
   //   .textFile("/user/cs422/lsh-query-6.csv")
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
    val exact : Construction = new ExactNN(sqlContext, rdd_corpus, threshold = 0.30)


val lsh : Construction = new ANDConstruction (List (
  new ORConstruction(List(new BaseConstructionBroadcast(sqlContext, rdd_corpus), new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
  new ORConstruction(List(new BaseConstructionBroadcast(sqlContext, rdd_corpus), new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
  new ORConstruction(List(new BaseConstructionBroadcast(sqlContext, rdd_corpus), new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
  new ORConstruction(List(new BaseConstructionBroadcast(sqlContext, rdd_corpus), new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
  new ORConstruction(List(new BaseConstructionBroadcast(sqlContext, rdd_corpus), new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
  new ORConstruction(List(new BaseConstructionBroadcast(sqlContext, rdd_corpus), new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
  new ORConstruction(List(new BaseConstructionBroadcast(sqlContext, rdd_corpus), new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus),new BaseConstructionBroadcast(sqlContext, rdd_corpus)))



))
    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    val rec = recall(ground, res)
    val prec = precision(ground, res)
    println("Recall: ",rec," Prec: ",prec)
    //val distances = res.join(ground).map(x=>(x._1, x._2._1.intersect(x._2._2).size/x._2._1.union(x._2._2).size.toDouble))
   // val tp = distances.map(_._2).reduce(_ + _)
   // println(tp/res.count())

 //   assert(recall(ground, res) > 0.7)
  //  assert(precision(ground, res) > 0.98)
  }

  def query2 (sc : SparkContext, sqlContext : SQLContext) : Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-1.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact : Construction = new ExactNN(sqlContext, rdd_corpus, threshold = 0.3)

    val lsh : Construction = new ORConstruction (List(
      new BaseConstruction(sqlContext, rdd_corpus),
      new BaseConstruction(sqlContext, rdd_corpus),
      new BaseConstruction(sqlContext, rdd_corpus)
    ))

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)
    println("query2 recall:", recall(ground, res))
    println("query2 precision:", precision(ground, res))
    val distances = res.join(ground).map(x=>(x._1, x._2._1.intersect(x._2._2).size/x._2._1.union(x._2._2).size.toDouble))
    val tp = distances.map(_._2).reduce(_ + _)
    //println(tp/res.count())
    //assert(recall(ground, res) > 0.9)
    //assert(precision(ground, res) > 0.45)
  }

  def query0 (sc : SparkContext, sqlContext : SQLContext) : Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      //.textFile("/user/cs422/lsh-corpus-large.csv")
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-0.csv").getFile).getPath

      val rdd_query = sc
      .textFile(query_file)
     // .textFile("/user/cs422/lsh-query-7.csv")
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact : Construction = new ExactNN(sqlContext, rdd_corpus, threshold = 0.3)

    val lsh : Construction = new BaseConstruction(sqlContext, rdd_corpus)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)
    //res.count()
    //ground.count()
     println("query0 recall:", recall(ground, res))
     println("query0 precision:", precision(ground, res))

    val distances = res.join(ground).map(x=>(x._1, x._2._1.intersect(x._2._2).size/x._2._1.union(x._2._2).size.toDouble))
    val tp = distances.map(_._2).reduce(_ + _)
    println(tp/res.count())
    //assert(recall(ground, res) > 0.83)
    //assert(precision(ground, res) > 0.70)
  }


  def main(args: Array[String]) {
    val t1 = System.nanoTime
    val conf = new SparkConf().setAppName("app")
    .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    query0(sc, sqlContext)
    query1(sc, sqlContext)
    query2(sc, sqlContext)
    val duration = (System.nanoTime - t1) / 1e9d
    println("Duration: ", duration)
  }     
}


