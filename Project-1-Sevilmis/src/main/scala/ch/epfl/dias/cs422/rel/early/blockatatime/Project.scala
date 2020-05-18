package ch.epfl.dias.cs422.rel.early.blockatatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

class Project protected (input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  protected var proj_iter : Iterator[Tuple] = _
  protected var proj_elem : Tuple = _
  protected var table_store : IndexedSeq[Tuple] = _
  protected var blk_size : Long = blockSize
  protected var proj_ctr : Int = 0

  lazy val evaluator: Tuple => Tuple =
  {
    var tmp : IndexedSeq[RexNode] = IndexedSeq[RexNode]()
    for(i <- 0 until projects.size())
    {
      tmp = tmp :+ projects.get(i)
    }
    eval(tmp, input.getRowType)
  }

  override def open(): Unit = {
    proj_iter = input.iterator
    table_store = IndexedSeq[Tuple]()
    while(proj_iter.hasNext)
    {
      proj_elem = proj_iter.next()
      if(proj_elem != null)
      {
        for(i <- proj_elem)
        {
          table_store = table_store :+ i.asInstanceOf[Tuple]
        }
      }
    }
  }

  override def next(): Block = {
    var res : Block = null
    if(proj_ctr < table_store.size)
    {
      res = IndexedSeq[Tuple]()
      if((proj_ctr + blk_size).asInstanceOf[Int] > table_store.size)
      {
        blk_size = table_store.size - proj_ctr
      }
      for(i <- proj_ctr until (proj_ctr + blk_size).asInstanceOf[Int])
      {
        res = res :+ evaluator(table_store(i))
      }
      proj_ctr += blk_size.asInstanceOf[Int]
    }
    res
  }

  override def close(): Unit = {
    input.close()
  }
}
