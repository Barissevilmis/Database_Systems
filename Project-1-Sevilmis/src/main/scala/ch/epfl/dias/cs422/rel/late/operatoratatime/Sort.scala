package ch.epfl.dias.cs422.rel.late.operatoratatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.{LazyEvaluatorAccess, LazyEvaluatorRoot}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  protected var iter : Iterator[Column] = _
  protected var scan_elem : Column = _
  protected var table_storage : IndexedSeq[Column] = IndexedSeq[Column]()
  protected var table_sorted : IndexedSeq[IndexedSeq[Any]] = _
  protected var table_tmp : IndexedSeq[IndexedSeq[Any]] = _
  protected var table_result : IndexedSeq[Column] = IndexedSeq[Column]()
  protected var tmp_res : IndexedSeq[Column] = IndexedSeq[Column]()
  protected var sort_res : IndexedSeq[Column] = _
  protected var sort_criteria : IndexedSeq[Boolean] = IndexedSeq[Boolean]()
  protected var sort_mask : Int = -1
  protected var scan_iter : Int = 0
  protected var mask : Boolean = _
  protected var el1_comp : Comparable[Any] = _
  protected var el2_comp : Comparable[Any] = _
  protected var coll_iter :  util.List[RelFieldCollation] = _
  protected var table_size : Int = 0

  def SortVIDs() : Boolean = {
    sort_mask += 1
    sort_criteria(sort_mask)
  }

  def SortAccCollation(el1 : Tuple, el2 : Tuple, coll : RelCollation): Boolean = {

    coll_iter = coll.getFieldCollations
    for(i <- 0 until coll_iter.size())
    {
      el1_comp = el1(coll_iter.get(i).getFieldIndex).asInstanceOf[Comparable[Any]]
      el2_comp = el2(coll_iter.get(i).getFieldIndex).asInstanceOf[Comparable[Any]]
      if(el1_comp.compareTo(el2_comp) != 0)
      {
        mask = el1_comp.compareTo(el2_comp) > 0
        if(coll_iter.get(i).getDirection.isDescending)
        {
          sort_criteria = sort_criteria :+ mask
          return mask
        }
        else
        {
          sort_criteria = sort_criteria :+ !mask
          return !mask
        }
      }
    }
    sort_criteria = sort_criteria :+ false
    false
  }


  private lazy val evals_sort: LazyEvaluatorRoot= input.evaluators()

  override def evaluators(): LazyEvaluatorRoot = evals_sort

  override def execute(): IndexedSeq[Column] = {
    var res : IndexedSeq[Column] = IndexedSeq[Column]()
    iter = input.iterator
    while (iter.hasNext)
    {
      scan_elem = iter.next()
      if (scan_elem != null)
      {
        table_storage = table_storage :+ input.evaluators().apply(scan_elem)
        tmp_res = tmp_res :+ scan_elem
      }
    }

    table_size = table_storage.size
    table_sorted = table_storage.sortWith((elem1, elem2) => SortAccCollation(elem1, elem2, collation))
    sort_res = tmp_res.sortWith((_,_) => SortVIDs())

    if (offset != null)
    {
      if (table_size > scan_iter + evalLiteral(fetch).asInstanceOf[Int])
      {
        scan_iter += evalLiteral(offset).asInstanceOf[Int]
      }
    }
    if (fetch != null)
    {
      if (table_size > scan_iter + evalLiteral(fetch).asInstanceOf[Int])
      {
        table_size = scan_iter + evalLiteral(fetch).asInstanceOf[Int]
      }
    }
    for (i <- scan_iter until table_size)
    {
      table_result = table_result :+ table_sorted(i)
      res = res :+ sort_res(i)
    }
    table_result = table_result.transpose
    res
  }
}
