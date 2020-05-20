package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.math.sqrt
import scala.util.Sorting.quickSort
import org.slf4j.LoggerFactory

class ThetaJoin(partitions: Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("ThetaJoin")

  /*
  this method takes as input two datasets (dat1, dat2) and returns the pairs of join keys that satisfy the theta join condition.
  attrIndex1: is the index of the join key of dat1
  attrIndex2: is the index of the join key of dat2
  condition: Is the join condition. Assume only "<", ">" will be given as input
  Assume condition only on integer fields.
  Returns and RDD[(Int, Int)]: projects only the join keys.
   */

  def ThetaJoin_Local(dat1:Array[Int], dat2:Array[Int], condition:String): Array[(Int, Int)] = {
    var res : Array[(Int, Int)] = null

    if(condition.equals("<"))
    {
      res = dat1.flatMap(x => dat2.map(y => (x, y))).filter(x => x._1 < x._2)
    }
    else
    {
      res = dat1.flatMap(x => dat2.map(y => (x, y))).filter(x => x._1 > x._2)
    }
    res
  }


  def ineq_join(dat1: RDD[Row], dat2: RDD[Row], attrIndex1: Int, attrIndex2: Int, condition:String): RDD[(Int, Int)] = {

    val dat1_map : RDD[Int] = dat1.map(tuple => tuple.getInt(attrIndex1))
    val dat2_map : RDD[Int] = dat2.map(tuple => tuple.getInt(attrIndex2))

    //TODO: PART (a) completed
    val blk_size = sqrt(dat1_map.count()* dat2_map.count() / partitions).toInt
    val c_r = dat1_map.count().toInt / blk_size
    val c_s = dat2_map.count().toInt / blk_size
    //println("Partitions c_r, c_s, partitions, blk_size: ",c_r,c_s,partitions, blk_size, dat1_map.count() * dat2_map.count(), sqrt(dat1_map.count() * dat2_map.count() / partitions).toInt)

    //TODO: PART (b) completed
    //var tmp_hor_bound = dat1_map.distinct.takeSample(withReplacement = false, c_r - 1)
    //var tmp_ver_bound = dat2_map.distinct.takeSample(withReplacement = false, c_s - 1)
    //FOR BETTER BOUNDARIES: BETTER PARTITIONING BETWEEN REDUCERS: OPTIMIZES NULL BOUNDARY REGIONS(EXCEPTIONAL CASE)
    val chc1 = (dat1_map.count()/c_r).toInt
    val chc2 = (dat2_map.count()/c_s).toInt

    var hor_bound_chc : List[Long] = null
    var ver_bound_chc : List[Long] = null

    if(dat1_map.count() == 1)
      hor_bound_chc = List.range(chc1, dat1_map.count(),chc1)
    else
      hor_bound_chc = List.range(chc1, dat1_map.count()-1,chc1)

    if(dat2_map.count() == 1)
      ver_bound_chc = List.range(chc2, dat2_map.count(),chc2)
    else
      ver_bound_chc = List.range(chc2, dat2_map.count()-1,chc2)


    var tmp_hor_bound = dat1_map.collect()
    var tmp_ver_bound = dat2_map.collect()

    quickSort(tmp_hor_bound)
    quickSort(tmp_ver_bound)

    tmp_hor_bound = tmp_hor_bound.zipWithIndex.filter(x => hor_bound_chc.contains(x._2)).map(x => x._1)
    tmp_ver_bound = tmp_ver_bound.zipWithIndex.filter(y => ver_bound_chc.contains(y._2)).map(y => y._1)

    val hor_bound = tmp_hor_bound.toIndexedSeq
    val ver_bound = tmp_ver_bound.toIndexedSeq

    //FINAL UNION ONLY TO ACQUIRE ALL INDICES BETWEEN BOUNDARIES
    //val rdd_hor_bound: RDD[(Int,Int)] = dat1_map.filter(el1 => hor_bound.contains(el1)).zipWithIndex.map(y => (y._1,y._2.asInstanceOf[Int]))
    //val rdd_ver_bound: RDD[(Int,Int)] = dat2_map.filter(el2 => ver_bound.contains(el2)).zipWithIndex.map(y => (y._1,y._2.asInstanceOf[Int]))
    val rdd_hor_bound: RDD[(Int,Int)] = dat1_map.distinct.filter(el1 => hor_bound.contains(el1))
      .union(dat1_map.zipWithIndex.filter(x => x._2 == 0).map(x => x._1))
      .zipWithIndex.map(y => (y._1,y._2.toInt))
    val rdd_ver_bound: RDD[(Int,Int)] = dat2_map.distinct.filter(el2 => ver_bound.contains(el2))
      .union(dat1_map.zipWithIndex.filter(x => x._2 == 0).map(x => x._1))
      .zipWithIndex.map(y  => (y._1,y._2.toInt))


    val hor_bound_extend = 0 +: hor_bound :+ Int.MaxValue
    val ver_bound_extend = 0 +: ver_bound :+ Int.MaxValue

    println(hor_bound_extend)

    var hor_prune : IndexedSeq[IndexedSeq[Boolean]] = IndexedSeq[IndexedSeq[Boolean]]()
    var ver_prune : IndexedSeq[IndexedSeq[Boolean]] = IndexedSeq[IndexedSeq[Boolean]]()

    // TODO: PRUNING PARTITIONS
    if(condition.equals(">"))
    {
      for(i <- 1 until hor_bound_extend.length)
      {
        var tmp_hor_prune : IndexedSeq[Boolean] = IndexedSeq[Boolean]()
        var tmp_ver_prune : IndexedSeq[Boolean] = IndexedSeq[Boolean]()
        for(j <- 0 until ver_bound_extend.length-1)
        {

          if(hor_bound_extend(i) >= ver_bound_extend(j))
            tmp_hor_prune = tmp_hor_prune :+ true
          else
            tmp_hor_prune = tmp_hor_prune :+ false

          if(ver_bound_extend(j) > hor_bound_extend(i))
            tmp_ver_prune = tmp_ver_prune :+ false
          else
            tmp_ver_prune = tmp_ver_prune :+ true
        }
        hor_prune = hor_prune :+ tmp_hor_prune
        ver_prune = ver_prune :+ tmp_ver_prune
      }
      hor_prune = hor_prune.transpose
      /*
      if(dat1.count() <= dat2.count())
        hor_prune = hor_prune.transpose
      else
        ver_prune = ver_prune.transpose
      */
    }
    else
    {
      for(i <- 1 until ver_bound_extend.length)
      {
        var tmp_hor_prune : IndexedSeq[Boolean] = IndexedSeq[Boolean]()
        var tmp_ver_prune : IndexedSeq[Boolean] = IndexedSeq[Boolean]()
        for(j <- 0 until hor_bound_extend.length-1)
        {

          if(ver_bound_extend(i) >= hor_bound_extend(j))
            tmp_ver_prune = tmp_ver_prune :+ true
          else
            tmp_ver_prune = tmp_ver_prune :+ false

          if(hor_bound_extend(j) > ver_bound_extend(i))
            tmp_hor_prune = tmp_hor_prune :+ false
          else
            tmp_hor_prune = tmp_hor_prune :+ true
        }
        hor_prune = hor_prune :+ tmp_hor_prune
        ver_prune = ver_prune :+ tmp_ver_prune
      }
      ver_prune = ver_prune.transpose
      /*
      if(dat1.count() < dat2.count())
        hor_prune = hor_prune.transpose
      else
        ver_prune = ver_prune.transpose
       */
    }

    //TODO: PART (c) completed

    val hor_range = List.range(1, (c_r*c_s) + 1, c_s)
    val hor_filt = hor_bound_extend.indices.map(hor =>
      dat1_map.filter(elem =>
        (elem >= hor_bound_extend(hor)) && (elem < hor_bound_extend(hor + 1))
      )
    ).map(x => x.collect)

    var hor_region = rdd_hor_bound.map(hor =>
      List.range(hor_range(hor._2),hor_range(hor._2)+c_s).zipWithIndex.map(hor_reg =>
        (hor_reg._1, 1, hor_filt(hor._2), hor_prune(hor_reg._2)(hor._2))
      )
    )
    val ver_filt = ver_bound_extend.indices.map(ver =>
      dat2_map.filter(elem =>
        (elem >= ver_bound_extend(ver)) && (elem < ver_bound_extend(ver + 1))
      )
    ).map(x => x.collect)

    var ver_region = rdd_ver_bound.map(ver =>
      List.range(ver._2+1,c_r*c_s+1,c_s).zipWithIndex.map(ver_reg =>
        (ver_reg._1, 2, ver_filt(ver._2),ver_prune(ver_reg._2)(ver._2))
      )
    )
    //hor_region.foreach(x => x.foreach(y => println(y._3.toIndexedSeq)))
    //ver_region.foreach(x => x.foreach(y => println(y._3.toIndexedSeq)))

    // TODO: Part (d)

    hor_region = hor_region.map(x => x.filter(y => y._4))
    ver_region = ver_region.map(x => x.filter(y => y._4))
    val partition_range = hor_region.flatMap(x => x.groupBy(_._1)).union(ver_region.flatMap(x => x.groupBy(_._1))).groupByKey()
    val final_partition = partition_range.filter(x => x._2.toList.size > 1)


    val res : RDD[(Int, Int)] = final_partition.map(x =>
    {
      var tmp_dat1: Array[Int] = Array[Int]()
      var tmp_dat2: Array[Int] = Array[Int]()
      x._2.foreach(y =>
        y.foreach(z =>
          if (z._2 == 1) {
            tmp_dat1 = tmp_dat1 ++ z._3
          }
          else if (z._2 == 2) {
            tmp_dat2 = tmp_dat2 ++ z._3
          }
        )
      )
      //ASK ABOUT THIS PART THURSDAY
      ThetaJoin_Local(tmp_dat1, tmp_dat2, condition)
    }
    ).flatMap(x => x)
    //res.persist()
    res
  }
}
