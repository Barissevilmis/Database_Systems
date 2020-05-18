package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rex.RexNode

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  protected var iter : Iterator[Column] =_
  protected var table_storage : IndexedSeq[Column] = _
  protected var filter_elem : Column = _

  lazy val e: Tuple => Any = eval(condition, input.getRowType)

  override def execute(): IndexedSeq[Column] = {
    var res : IndexedSeq[Column] = IndexedSeq[Column]()
    table_storage = IndexedSeq[Column]()
    iter = input.iterator
    while(iter.hasNext)
    {
      filter_elem = iter.next()
      if(filter_elem != null)
      {
        table_storage = table_storage :+ filter_elem
      }
    }
    table_storage = table_storage.transpose
    for(i <- table_storage.indices)
    {
      if(e(table_storage(i)).asInstanceOf[Boolean])
      {
        res = res :+ table_storage(i)
      }
    }

  res.transpose
  }
}
