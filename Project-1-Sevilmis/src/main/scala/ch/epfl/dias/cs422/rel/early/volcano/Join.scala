package ch.epfl.dias.cs422.rel.early.volcano

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode
import scala.collection.mutable

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  protected var join_iter : Int = 0
  protected var left_iter : Iterator[Tuple] = _
  protected var left_elem : Tuple = _
  protected var left_elems : IndexedSeq[IndexedSeq[Any]] = _
  protected var tmp_key_elems: IndexedSeq[Any] = _
  protected var left_key_elems : IndexedSeq[IndexedSeq[Any]] = _
  protected var right_key_elems : IndexedSeq[IndexedSeq[Any]] = _
  protected var right_iter : Iterator[Tuple] = _
  protected var right_elem : Tuple = _
  protected var right_elems : IndexedSeq[IndexedSeq[Any]] = _
  protected var hash_table : mutable.HashMap[IndexedSeq[Any],IndexedSeq[Any]] = mutable.HashMap[IndexedSeq[Any],IndexedSeq[Any]]()
  protected var table_result : IndexedSeq[Tuple] = _
  protected var table_size : Int = 0
  protected var elem : Tuple = _
  protected var left_keys : IndexedSeq[Int] = getLeftKeys
  protected var right_keys : IndexedSeq[Int] = getRightKeys

  override def open(): Unit =
    {
      left_iter = left.iterator
      right_iter = right.iterator
      left_elems = IndexedSeq[IndexedSeq[Any]]()
      left_key_elems = IndexedSeq[IndexedSeq[Any]]()
      while(left_iter.hasNext)
      {
        left_elem = left_iter.next()
        left_elems = left_elems :+ left_elem
        tmp_key_elems = IndexedSeq[Any]()
        for(l_key <- left_keys)
        {
          tmp_key_elems = tmp_key_elems :+ left_elem(l_key)
        }
        left_key_elems = left_key_elems :+ tmp_key_elems
        for(el <- left_key_elems)
        {
          if (!hash_table.contains(el))
          {
            hash_table.update(el, IndexedSeq[Any]())
          }
        }
      }

      right_elems = IndexedSeq[IndexedSeq[Any]]()
      right_key_elems = IndexedSeq[IndexedSeq[Any]]()
      while(right_iter.hasNext)
      {
        right_elem = right_iter.next()
        right_elems = right_elems :+ right_elem
        tmp_key_elems = IndexedSeq[Any]()
        for(r_key <- right_keys)
        {
          tmp_key_elems = tmp_key_elems :+ right_elem(r_key)
        }
        right_key_elems = right_key_elems :+ tmp_key_elems
        if (hash_table.contains(tmp_key_elems))
        {
          hash_table.update(tmp_key_elems, hash_table(tmp_key_elems) :+ right_elem)
        }
      }
      table_result = IndexedSeq[Tuple]()
      elem = IndexedSeq[Any]()
      for(el <- left_key_elems.indices)
      {
        if (hash_table.contains(left_key_elems(el)) && hash_table(left_key_elems(el)).nonEmpty)
        {
          for (h_el <- hash_table(left_key_elems(el)))
          {
            table_result = table_result :+ (left_elems(el) ++ h_el.asInstanceOf[IndexedSeq[Any]])
            table_size += 1
          }
        }
      }
    }

  override def next(): Tuple =
    {
      var res : Tuple = null
      if(join_iter < table_size)
      {
        res = table_result(join_iter)
        join_iter += 1
      }
      res

    }

  override def close(): Unit =
    {
      left_key_elems = null
      right_elems = null
      right_key_elems = null
      table_result = null
      hash_table = null
      left.close()
      right.close()
    }
}
