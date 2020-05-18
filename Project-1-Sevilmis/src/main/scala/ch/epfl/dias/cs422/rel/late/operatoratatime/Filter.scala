package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.late.{Evaluator, LazyEvaluatorRoot}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  protected var iter : Iterator[Column] =_
  protected var filter_elem : Column = _

  lazy val e: Evaluator = eval(condition, input.getRowType, input.evaluators())

  override def evaluators(): LazyEvaluatorRoot = input.evaluators()

  override def execute(): IndexedSeq[Column] =
  {
    var res : IndexedSeq[Column] = IndexedSeq[Column]()
    iter = input.iterator
    while(iter.hasNext)
    {
      filter_elem = iter.next()
      if(filter_elem != null)
      {
        if(e(filter_elem).asInstanceOf[Boolean])
        {
          res = res :+ filter_elem
        }
      }
    }
    res
  }

}
