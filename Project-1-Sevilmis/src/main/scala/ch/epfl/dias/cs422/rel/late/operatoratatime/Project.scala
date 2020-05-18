package ch.epfl.dias.cs422.rel.late.operatoratatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.{LazyEvaluator, LazyEvaluatorRoot}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

class Project protected(input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  protected var iter : Iterator[Column] = _
  protected var proj_elem : Column = _

  private lazy val evals_proj : LazyEvaluator = {
    var tmp : IndexedSeq[RexNode] = IndexedSeq[RexNode]()

    for(i <- 0 until projects.size())
    {
      tmp = tmp :+ projects.get(i)
    }
    lazyEval(tmp, input.getRowType, input.evaluators())
  }
  override def evaluators(): LazyEvaluator = evals_proj

  override def execute(): IndexedSeq[Column] =
  {
    iter = input.iterator
    var res : IndexedSeq[Column] = IndexedSeq[Column]()
    while(iter.hasNext)
    {
      proj_elem = iter.next()
      if (proj_elem != null)
      {
        res = res :+ proj_elem
      }
    }
    res
  }
}
