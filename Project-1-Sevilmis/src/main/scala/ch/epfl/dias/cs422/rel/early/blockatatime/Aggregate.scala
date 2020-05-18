package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  protected var iter : Iterator[Tuple] = _
  protected var agg_iter : Int = 0
  protected var index : Int = 0
  protected var elem : Tuple = _
  protected var tmp_elem : Tuple = _
  protected var mask_key : Boolean = true
  protected var table_tmp : IndexedSeq[Tuple] = _
  protected var table_storage: IndexedSeq[(Any, Tuple)] = IndexedSeq[(Any, Tuple)]()
  protected var agg_map : Map[Any,Any] = _
  protected var agg_key : IndexedSeq[Any] = _
  protected var agg_val : IndexedSeq[Any] = _
  protected var total_row : Int = 0
  protected var table_result : IndexedSeq[Tuple] = IndexedSeq[Tuple]()
  protected var blk_size : Long = blockSize
  protected var container : IndexedSeq[Any] = _

  override def open(): Unit = {
    iter = input.iterator
    table_tmp = IndexedSeq[Tuple]()
    while(iter.hasNext)
    {
      tmp_elem = iter.next()
      if (tmp_elem != null)
      {
        for (i <- tmp_elem)
        {
          table_tmp = table_tmp :+ i.asInstanceOf[Tuple]
        }
      }
    }
    val groupKeys = groupSet.asList
    if(!groupKeys.isEmpty)
    {
      for(j <- table_tmp.indices)
      {
        elem = table_tmp(j)
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
      for(j <- table_tmp.indices)
      {
        elem = table_tmp(j)
        table_storage = table_storage :+ (0,elem)
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
  }

  override def next(): Block =
    {
      var res: Block = null
      if(agg_iter < table_result(0).size)
      {
        res = IndexedSeq[Tuple]()
        if ((agg_iter + blk_size).asInstanceOf[Int] > table_result(0).size)
        {
          blk_size = table_result(0).size - agg_iter
        }
        for (agg <- agg_iter until (agg_iter + blk_size).asInstanceOf[Int])
        {
          container = IndexedSeq[Any]()
          for(i <- table_result.indices)
          {
            container = container :+ table_result(i)(agg)
          }
          res = res :+ container
        }
        agg_iter += blk_size.asInstanceOf[Int]
      }
      res
    }

  override def close(): Unit = {
    mask_key = true
    iter = null
    agg_map = null
    agg_val = null
    agg_key = null
    table_storage = null
    table_result = null
    input.close()
  }
}
