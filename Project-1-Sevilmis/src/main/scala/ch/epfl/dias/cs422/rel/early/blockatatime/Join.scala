package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  protected var join_iter : Int = 0
  protected var blk_size : Long = blockSize
  protected var left_iter : Iterator[Tuple] = _
  protected var left_elem : Tuple = _
  protected var left_elems : IndexedSeq[IndexedSeq[Any]] = _
  protected var tmp_key_elems: IndexedSeq[Any] = _
  protected var left_key_elems : IndexedSeq[IndexedSeq[Any]] = _
  protected var right_key_elems : IndexedSeq[IndexedSeq[Any]] = _
  protected var right_iter : Iterator[Tuple] = _
  protected var right_elem : Tuple = _
  protected var right_elems : IndexedSeq[IndexedSeq[Any]] = _
  protected var tmp_left_store : IndexedSeq[Tuple] = _
  protected var tmp_right_store : IndexedSeq[Tuple] = _
  protected var left_elem_2 : Tuple = _
  protected var right_elem_2 : Tuple = _
  protected var hash_table : mutable.HashMap[IndexedSeq[Any],IndexedSeq[Any]] = mutable.HashMap[IndexedSeq[Any],IndexedSeq[Any]]()
  protected var table_result : IndexedSeq[Tuple] = _
  protected var table_size : Int = 0
  protected var elem : Tuple = _
  protected var left_keys : IndexedSeq[Int] = getLeftKeys
  protected var right_keys : IndexedSeq[Int] = getRightKeys

  override def open(): Unit = {
    left_iter = left.iterator
    right_iter = right.iterator
    tmp_left_store = IndexedSeq[Tuple]()
    tmp_right_store = IndexedSeq[Tuple]()
    while(left_iter.hasNext)
    {
      left_elem = left_iter.next()
      if (left_elem != null)
      {
        for (i <- left_elem)
        {
          tmp_left_store = tmp_left_store :+ i.asInstanceOf[Tuple]
        }
      }
    }
    while(right_iter.hasNext)
    {
      right_elem = right_iter.next()
      if (right_elem != null)
      {
        for (i <- right_elem)
        {
          tmp_right_store = tmp_right_store :+ i.asInstanceOf[Tuple]
        }
      }
    }

    left_elems = IndexedSeq[IndexedSeq[Any]]()
    left_key_elems = IndexedSeq[IndexedSeq[Any]]()
    for (i <- tmp_left_store.indices)
    {
      left_elem_2 = tmp_left_store(i)
      left_elems = left_elems :+ left_elem_2
      tmp_key_elems = IndexedSeq[Any]()
      for (l_key <- left_keys)
      {
        tmp_key_elems = tmp_key_elems :+ left_elem_2(l_key)
      }
      left_key_elems = left_key_elems :+ tmp_key_elems
      for (el <- left_key_elems)
      {
        if (!hash_table.contains(el))
        {
          hash_table.update(el, IndexedSeq[Any]())
        }
      }
    }

    right_elems = IndexedSeq[IndexedSeq[Any]]()
    right_key_elems = IndexedSeq[IndexedSeq[Any]]()
    for (i <- tmp_right_store.indices)
    {
      right_elem_2 = tmp_right_store(i)
      right_elems = right_elems :+ right_elem_2
      tmp_key_elems = IndexedSeq[Any]()
      for (r_key <- right_keys)
      {
        tmp_key_elems = tmp_key_elems :+ right_elem_2(r_key)
      }
      right_key_elems = right_key_elems :+ tmp_key_elems
      if (hash_table.contains(tmp_key_elems))
      {
        hash_table.update(tmp_key_elems, hash_table(tmp_key_elems) :+ right_elem_2)
      }
    }
    table_result = IndexedSeq[Tuple]()
    elem = IndexedSeq[Any]()
    for (el <- left_key_elems.indices)
    {
      if (hash_table.contains(left_key_elems(el)) && hash_table(left_key_elems(el)).nonEmpty)
      {
        for (h_el <- hash_table(left_key_elems(el)))
        {
          table_result = table_result :+ (left_elems(el) ++ h_el.asInstanceOf[Tuple])
          table_size += 1
        }
      }
    }
  }

  override def next(): Block =
    {
      var res: Block = null
      if(join_iter < table_size)
      {
        res = IndexedSeq[Tuple]()
        if ((join_iter + blk_size).asInstanceOf[Int] > table_size)
        {
          blk_size = table_size - join_iter
        }
        for (join <- join_iter until (join_iter + blk_size).asInstanceOf[Int])
        {
          res = res :+ table_result(join)
        }
        join_iter += blk_size.asInstanceOf[Int]
      }
      res
    }

  override def close(): Unit = {
    left_key_elems = null
    right_elems = null
    right_key_elems = null
    table_result = null
    hash_table = null
    left.close()
    right.close()
  }
}
