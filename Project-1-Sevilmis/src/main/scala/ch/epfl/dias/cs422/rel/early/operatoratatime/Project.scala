package ch.epfl.dias.cs422.rel.early.operatoratatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

class Project protected(input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  protected var table_store : IndexedSeq[Column] = _
  protected var iter : Iterator[Column] = _
  protected var proj_elem : Column = _

  lazy val evaluator: Tuple => Tuple =
  {
    var tmp : IndexedSeq[RexNode] = IndexedSeq[RexNode]()
    for(i <- 0 until projects.size())
    {
        tmp = tmp :+ projects.get(i)
    }
    eval(tmp, input.getRowType)
  }

  override def execute(): IndexedSeq[Column] = {
    iter = input.iterator
    var res : IndexedSeq[Column] = IndexedSeq[Column]()
    table_store = IndexedSeq[Column]()
    while(iter.hasNext)
    {
      proj_elem = iter.next()
      if(proj_elem != null)
      {
        table_store = table_store :+ proj_elem
      }
    }
    table_store = table_store.transpose
    for (i <- table_store.indices)
    {
      res = res :+ evaluator(table_store(i))
    }
    res.transpose
  }
}
