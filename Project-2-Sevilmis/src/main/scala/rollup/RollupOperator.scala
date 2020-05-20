package rollup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class RollupOperator() {

  /*
 * This method gets as input one dataset, the indexes of the grouping attributes of the rollup (ROLLUP clause)
 * the index of the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = List[Any], value = Double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */

  def MinMaxFunc(agg : String) : (Double, Double) => Double =
  {
    (val1: Double, val2: Double) =>
    {
      if (agg.equals("MIN"))
      {
        if (val1 < val2)
        {
          val1
        }
        else
        {
          val2
        }
      }
      else
      {
        if (val1 < val2)
        {
          val2
        }
        else
        {
          val1
        }
      }
    }
  }
  def AvgFunc : ((Double, Double), (Double, Double)) => (Double, Double) =
  {
    (val1: (Double, Double), val2: (Double, Double)) => (val1._1 + val2._1, val1._2 + val2._2)
  }

  def SumCountFunc(agg : String) : (Double, Double) => Double =
  {
    (val1: Double, val2: Double) => val1 + val2
  }

  def rollup(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {
    //TODO Task 1 : Completed
    val gr_len : Int = groupingAttributeIndexes.length

    if(agg.equals("COUNT") || agg.equals("SUM"))
    {
      val group_attr_res_init: RDD[(List[Any], Double)] = dataset.map(x =>
        if (agg.equals("COUNT"))
          (groupingAttributeIndexes.map(ind => x(ind)), 1.toDouble)
        else
          (groupingAttributeIndexes.map(ind => x(ind)), x.getInt(aggAttributeIndex).toDouble)
      )

      var group_attr_res_final : RDD[(List[Any], Double)] = group_attr_res_init.map(x => (x._1.combinations(gr_len).toList.head,x._2)).reduceByKey(SumCountFunc(agg))
      var res_final : RDD[(List[Any], Double)] = group_attr_res_final

      for(k <- 0 until gr_len)
      {
        group_attr_res_final = group_attr_res_final.map(x => (x._1.combinations(gr_len-k-1).toList.head,x._2)).reduceByKey(SumCountFunc(agg))
        res_final = res_final.union(group_attr_res_final)
      }
      res_final
    }
    else if(agg.equals("MIN") || agg.equals("MAX"))
    {
      val group_attr_res_init: RDD[(List[Any], Double)] = dataset.map(x => (groupingAttributeIndexes.map(ind => x(ind)), x.getInt(aggAttributeIndex).toDouble))
      var group_attr_res_final : RDD[(List[Any], Double)] = group_attr_res_init.map(x => (x._1.combinations(gr_len).toList.head,x._2)).reduceByKey(MinMaxFunc(agg))
      var res_final : RDD[(List[Any], Double)] = group_attr_res_final

      for(k <- 0 until gr_len)
      {
        group_attr_res_final = group_attr_res_final.map(x => (x._1.combinations(gr_len-k-1).toList.head,x._2)).reduceByKey(MinMaxFunc(agg))
        res_final = res_final.union(group_attr_res_final)
      }
      res_final
    }
    else
    {
      val group_attr_res_init: RDD[(List[Any], (Double, Double))] = dataset.map(x => (groupingAttributeIndexes.map(ind => x(ind)), (x.getInt(aggAttributeIndex).toDouble, 1.toDouble)))
      var group_attr_res_final : RDD[(List[Any], (Double,Double))] = group_attr_res_init.map(x => (x._1.combinations(gr_len).toList.head,x._2)).reduceByKey(AvgFunc)
      var res_final : RDD[(List[Any], Double)] = group_attr_res_final.mapValues(x => x._1 / x._2)

      for(k <- 0 until gr_len)
      {
        group_attr_res_final = group_attr_res_final.map(x => (x._1.combinations(gr_len-k-1).toList.head,x._2)).reduceByKey(AvgFunc)
        res_final = res_final.union(group_attr_res_final.mapValues(x => x._1 / x._2))
      }
      res_final
    }


    /*
    if(agg.equals("AVG"))
    {
      res_init = group_attr_res_init.asInstanceOf[RDD[(List[Any], (Double, Double))]].
        reduceByKey(AvgFunc).mapValues(x => x._1 / x._2)
    }
    else if(agg.equals("MIN") || agg.equals("MAX"))
    {
      res_init = group_attr_res_init.asInstanceOf[RDD[(List[Any], Double)]].
        reduceByKey(MinMaxFunc(agg))
    }
    else if(agg.equals("SUM") || agg.equals("COUNT"))
    {
      res_init = group_attr_res_init.asInstanceOf[RDD[(List[Any], Double)]].
        reduceByKey(SumCountFunc(agg))
    }
    */



/*
    val group_attr_res_final  = group_attr_res_init.flatMap(x => (0 to groupingAttributeIndexes.length).toList.
      map(y => (x_1.combinations(y).toList.head,x._2)))

    if(agg.equals("AVG"))
    {
      res_final = group_attr_res_final.asInstanceOf[RDD[(List[Any], (Double, Double))]].
        reduceByKey(AvgFunc).mapValues(x => x._1 / x._2)
    }
    else if(agg.equals("MIN") || agg.equals("MAX"))
    {
      res_final = group_attr_res_final.asInstanceOf[RDD[(List[Any], Double)]].
        reduceByKey(MinMaxFunc(agg))
    }
    else if(agg.equals("SUM") || agg.equals("COUNT"))
    {
      res_final = group_attr_res_final.asInstanceOf[RDD[(List[Any], Double)]].
        reduceByKey(SumCountFunc(agg))
    }
    res_final
*/
  }

  def rollup_naive(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {
    //TODO naive algorithm for cube computation
    var res : RDD[(List[Any], Double)] = null

    val group_attr_res: RDD[Any] = dataset.flatMap(x => (0 to groupingAttributeIndexes.length).toList.map(y =>
      if (agg.equals("COUNT"))
      {
        (groupingAttributeIndexes.map(z => x(z)).combinations(y).toList.head, 1.asInstanceOf[Double])
      }
      else if (agg.equals("AVG"))
      {
        (groupingAttributeIndexes.map(z => x(z)).combinations(y).toList.head,
          (x.getInt(aggAttributeIndex).asInstanceOf[Double], 1.asInstanceOf[Double]))
      }
      else if (agg.equals("SUM") || agg.equals("MIN") || agg.equals("MAX"))
      {
        (groupingAttributeIndexes.map(z => x(z)).combinations(y).toList.head, x.getInt(aggAttributeIndex).asInstanceOf[Double])
      }
    )
    )
    if(agg.equals("AVG"))
    {
      res = group_attr_res.asInstanceOf[RDD[(List[Any], (Double, Double))]].
        reduceByKey(AvgFunc).mapValues(x => x._1 / x._2)
    }
    else if(agg.equals("MIN") || agg.equals("MAX"))
    {
      res = group_attr_res.asInstanceOf[RDD[(List[Any], Double)]].
        reduceByKey(MinMaxFunc(agg))
    }
    else if(agg.equals("SUM") || agg.equals("COUNT"))
    {
      res = group_attr_res.asInstanceOf[RDD[(List[Any], Double)]].
        reduceByKey(SumCountFunc(agg))
    }
    res
  }
}

