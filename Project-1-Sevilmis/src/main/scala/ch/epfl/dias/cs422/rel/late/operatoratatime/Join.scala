package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.{LazyEvaluatorAccess, LazyEvaluatorRoot}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  protected var join_iter : Int = 0
  protected var left_iter : Iterator[Column] = _
  protected var left_elem : Column = _
  protected var left_elem_2 : Tuple = _
  protected var tmp_left_store : IndexedSeq[Column] = _
  protected var tmp_key_elems: IndexedSeq[Any] = _
  protected var left_key_elems : IndexedSeq[IndexedSeq[Any]] = _
  protected var right_key_elems : IndexedSeq[IndexedSeq[Any]] = _
  protected var right_iter : Iterator[Column] = _
  protected var right_elem : Column = _
  protected var right_elem_2 : Tuple = _
  protected var tmp_right_store : IndexedSeq[Column] = _
  protected var hash_table : mutable.HashMap[IndexedSeq[Any],IndexedSeq[Any]] = mutable.HashMap[IndexedSeq[Any],IndexedSeq[Any]]()
  protected var left_keys : IndexedSeq[Int] = getLeftKeys
  protected var right_keys : IndexedSeq[Int] = getRightKeys

  private lazy val evals_join = lazyEval(left.evaluators(), right.evaluators(), left.getRowType, right.getRowType)

  override def evaluators(): LazyEvaluatorRoot = evals_join

  override def execute(): IndexedSeq[Column] = {

    var res : IndexedSeq[Column] = IndexedSeq[Column]()
    left_iter = left.iterator
    right_iter = right.iterator
    tmp_left_store = IndexedSeq[Column]()
    tmp_right_store = IndexedSeq[Column]()
    while (left_iter.hasNext)
    {
      left_elem = left_iter.next()
      if (left_elem != null)
      {
        tmp_left_store = tmp_left_store :+ left_elem
      }
    }
    while (right_iter.hasNext)
    {
      right_elem = right_iter.next()
      if (right_elem != null)
      {
        tmp_right_store = tmp_right_store :+ right_elem
      }
    }

    left_key_elems = IndexedSeq[IndexedSeq[Any]]()
    for (i <- tmp_left_store.indices)
    {
      left_elem_2 = left.evaluators().apply(tmp_left_store(i))
      tmp_key_elems = IndexedSeq[Any]()
      for (l_key <- left_keys)
      {
        tmp_key_elems = tmp_key_elems :+ left_elem_2(l_key)
      }
      left_key_elems = left_key_elems :+ tmp_key_elems
      for (el <- left_key_elems.indices)
      {
        if (!hash_table.contains(left_key_elems(el)))
        {
          hash_table.update(left_key_elems(el), IndexedSeq[Any]())
        }
      }
    }
    right_key_elems = IndexedSeq[IndexedSeq[Any]]()
    for (i <- tmp_right_store.indices)
    {
      right_elem_2 = right.evaluators().apply(tmp_right_store(i))
      tmp_key_elems = IndexedSeq[Any]()
      for (r_key <- right_keys)
      {
        tmp_key_elems = tmp_key_elems :+ right_elem_2(r_key)
      }
      right_key_elems = right_key_elems :+ tmp_key_elems
      if (hash_table.contains(tmp_key_elems))
      {
        hash_table.update(tmp_key_elems, hash_table(tmp_key_elems) :+ tmp_right_store(i))
      }
    }
    for (el <- left_key_elems.indices)
    {
      if (hash_table.contains(left_key_elems(el)) && hash_table(left_key_elems(el)).nonEmpty)
      {
        for (h_el <- hash_table(left_key_elems(el)))
        {
          res = res :+ (IndexedSeq(tmp_left_store(el)) :+ h_el.asInstanceOf[IndexedSeq[Column]])
        }
      }
    }
    res
  }
}
