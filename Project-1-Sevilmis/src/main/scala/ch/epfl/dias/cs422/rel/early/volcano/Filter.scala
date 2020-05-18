package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import org.apache.calcite.rex.RexNode

class Filter protected (input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  protected var iter: Iterator[Tuple] = _

  override def open(): Unit = {
    iter = input.iterator
  }
  lazy val e: Tuple => Any = eval(condition, input.getRowType)

  override def next(): Tuple = {
    var res : Tuple = null
    while(iter.hasNext)
    {
      res = iter.next()
      if(e(res).asInstanceOf[Boolean])
        {
          return res
        }
    }
    null
  }

  override def close(): Unit = {
    input.close()
  }
}
