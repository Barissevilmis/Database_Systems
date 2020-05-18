package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

class Project protected (input: Operator, projects: java.util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  protected var iter: Iterator[Tuple] = _

  override def open(): Unit = {
    iter = input.iterator
    }
  lazy val evaluator: Tuple => Tuple = eval(projects.asScala.toIndexedSeq, input.getRowType)
  override def next(): Tuple = {
    var res : Tuple = null
    while(iter.hasNext)
    {
      res = iter.next()
      if(evaluator(res) != null)
      {
        return evaluator(res)
      }
    }
    null
  }

  override def close(): Unit = {
    input.close()
  }
}
