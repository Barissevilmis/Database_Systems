package ch.epfl.dias.cs422.rel.early.volcano

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode

class Sort protected (input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  protected var iter : Iterator[Tuple] = _
  protected var scan_elem : IndexedSeq[Any] = _
  protected var table_storage : IndexedSeq[IndexedSeq[Any]] = _
  protected var table_sorted : IndexedSeq[IndexedSeq[Any]] = _
  protected var scan_iter : Int = 0
  protected var mask : Boolean = _
  protected var el1_comp : Comparable[Any] = _
  protected var el2_comp : Comparable[Any] = _
  protected var coll_iter :  util.List[RelFieldCollation] = _
  protected var table_size : Int = 0

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
            return mask
          }
          else
          {
            return !mask
          }
        }
      }
    false
  }
  override def open(): Unit = {
    iter = input.iterator
    table_storage = IndexedSeq[IndexedSeq[Any]]()
    while(iter.hasNext)
    {
      scan_elem = iter.next()
      table_storage = table_storage :+ scan_elem
      table_size += 1
    }
    table_sorted = table_storage.sortWith((elem1,elem2) => SortAccCollation(elem1,elem2,collation))
    if(offset != null)
    {
      if(table_size > scan_iter + evalLiteral(fetch).asInstanceOf[Int])
      {
        scan_iter += evalLiteral(offset).asInstanceOf[Int]
      }
    }
    if(fetch != null)
    {
      if(table_size > scan_iter + evalLiteral(fetch).asInstanceOf[Int])
      {
        table_size = scan_iter + evalLiteral(fetch).asInstanceOf[Int]
      }
    }

  }

  override def next(): Tuple = {
    var res : Tuple = null
    if(scan_iter < table_size)
    {
      res = table_sorted(scan_iter)
      scan_iter += 1
    }
    res
  }

  override def close(): Unit = {
    table_sorted = null
    table_storage = null
    input.close()
  }
}
