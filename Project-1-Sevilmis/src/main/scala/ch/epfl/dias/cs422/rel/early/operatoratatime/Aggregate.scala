package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

class Aggregate protected(input: Operator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  protected var iter : Iterator[Column] = _
  protected var agg_iter : Int = 0
  protected var index : Int = 0
  protected var elem : Column = _
  protected var elem_2 : Tuple = _
  protected var mask_key : Boolean = true
  protected var table_tmp : IndexedSeq[Column] = _
  protected var table_storage: IndexedSeq[(Any, Column)] = IndexedSeq[(Any, Column)]()
  protected var agg_map : Map[Any,Any] = _
  protected var agg_key : IndexedSeq[Any] = _
  protected var agg_val : IndexedSeq[Any] = _
  protected var total_row : Int = 0
  protected var table_result : IndexedSeq[Tuple] = IndexedSeq[Tuple]()
  override def execute(): IndexedSeq[Column] = {
    iter = input.iterator
    val groupKeys = groupSet.asList

    table_tmp = IndexedSeq[Column]()
    while (iter.hasNext)
    {
      elem = iter.next()
      if(elem != null)
      {
        table_tmp = table_tmp :+ elem
      }

    }
    table_tmp = table_tmp.transpose

    if (!groupKeys.isEmpty)
    {
      for (i <- table_tmp.indices)
      {
        elem_2 = table_tmp(i)
        var group_actual_keys: IndexedSeq[Any] = IndexedSeq[Any]()
        for (i <- 0 until groupKeys.size())
        {
          group_actual_keys = group_actual_keys :+ elem_2(groupKeys.get(i))
        }
        table_storage = table_storage :+ (group_actual_keys, elem_2)
      }
    }
    else
    {
      for(i <- table_tmp.indices)
      {
        elem_2 = table_tmp(i)
        table_storage = table_storage :+ (0, elem_2)
      }
    }
    if (!groupSet.isEmpty && aggCalls.isEmpty)
    {
      agg_map = table_storage.groupBy(item => item._1)
      agg_key = agg_map.keys.toIndexedSeq
      if (agg_map.nonEmpty)
      {
        var curr_tmp : IndexedSeq[Any] = null
        for(i <- agg_key(0).asInstanceOf[Vector[Any]].indices)
        {
          curr_tmp = IndexedSeq[Any]()
          for(j <- agg_key.indices)
          {
            curr_tmp = curr_tmp :+ agg_key(j).asInstanceOf[Vector[Any]](i)
          }
          table_result = table_result :+ curr_tmp
        }
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
          if (!groupSet.isEmpty && mask_key)
          {
            var curr_tmp : IndexedSeq[Any] = null
            for(i <- agg_key(0).asInstanceOf[Vector[Any]].indices)
            {
              curr_tmp = IndexedSeq[Any]()
              for(j <- agg_key.indices)
              {
                curr_tmp = curr_tmp :+ agg_key(j).asInstanceOf[Vector[Any]](i)
              }
              table_result = table_result :+ curr_tmp
            }
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
    table_result
  }
}
