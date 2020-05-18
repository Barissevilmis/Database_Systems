package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  protected var iter : Iterator[Tuple] = _
  protected var agg_iter : Int = 0
  protected var index : Int = 0
  protected var elem : Tuple = _
  protected var mask_key : Boolean = true
  protected var table_storage: IndexedSeq[(Any, Tuple)] = IndexedSeq[(Any, Tuple)]()
  protected var agg_map : Map[Any,Any] = _
  protected var agg_key : IndexedSeq[Any] = _
  protected var agg_val : IndexedSeq[Any] = _
  protected var total_row : Int = 0
  protected var table_result : IndexedSeq[Tuple] = IndexedSeq[Tuple]()


  override def open(): Unit = {
    iter = input.iterator
    val groupKeys = groupSet.asList
    if(!groupKeys.isEmpty)
    {
      while(iter.hasNext)
      {
        elem = iter.next()
        var group_actual_keys : IndexedSeq[Any] = IndexedSeq[Any]()
        for(i <- 0 until groupKeys.size())
        {
            group_actual_keys = group_actual_keys :+ elem(groupKeys.get(i))
        }
        table_storage = table_storage :+ (group_actual_keys,elem)
      }
    }
    else
    {
      while(iter.hasNext)
      {
        elem = iter.next()
        table_storage = table_storage :+ (0,elem)
      }
    }
    if(!groupSet.isEmpty && aggCalls.isEmpty)
    {
      agg_map = table_storage.groupBy(item => item._1)
      agg_key = agg_map.keys.toIndexedSeq
      if(agg_map.nonEmpty)
      {
        table_result = table_result :+ agg_key
        mask_key = false
      }
      else
      {
        table_result = table_result :+ IndexedSeq(0)
      }
    }
    else
    {
      for (i <- aggCalls.indices)
      {
        agg_map = table_storage.groupMapReduce(item => item._1)(element => aggCalls(i).getArgument(element._2))((elem1, elem2) => aggCalls(i).reduce(elem1, elem2))
        agg_key = agg_map.keys.toIndexedSeq
        agg_val = agg_map.values.toIndexedSeq

        if (agg_map.nonEmpty)
        {
          if(!groupSet.isEmpty && mask_key)
          {
            table_result = table_result :+ agg_key
            table_result = table_result :+ agg_val
            mask_key = false
          }
          else
          {
            table_result = table_result :+ agg_val
          }
        }
        else
        {
          table_result = table_result :+ IndexedSeq(0)
        }
      }
    }
  }

  override def next(): Tuple = {
    var res : Tuple = null

    if(table_result.nonEmpty)
    {
      total_row = table_result(0).length
    }
    if(agg_iter < total_row)
    {
      res = IndexedSeq[Any]()
      for(i <- table_result.indices)
      {
        if(i == 0 && !mask_key)
        {
          for(j <- table_result(i)(agg_iter).asInstanceOf[Vector[Any]])
          {
            res = res :+ j
          }
        }
        else
        {
          res = res :+ table_result(i)(agg_iter)
        }
      }
      agg_iter += 1
    }
    res
  }

  override def close(): Unit = {
    input.close()
    mask_key = true
    iter = null
    agg_map = null
    agg_val = null
    agg_key = null
    table_storage = null
    table_result = null
  }
}
