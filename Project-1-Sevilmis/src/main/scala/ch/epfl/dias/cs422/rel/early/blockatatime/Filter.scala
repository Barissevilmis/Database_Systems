package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode

class Filter protected (input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  protected var filter_iter : Iterator[Tuple] = _
  protected var table_store : IndexedSeq[Tuple] = _
  protected var filter_elem : Tuple = _
  protected var filter_ctr : Int = 0
  protected var blk_size : Long = blockSize

  lazy val e: Tuple => Any = eval(condition, input.getRowType)

  override def open(): Unit = {
    filter_iter = input.iterator
    table_store = IndexedSeq[Tuple]()
    while(filter_iter.hasNext)
    {
      filter_elem = filter_iter.next()
      if(filter_elem != null)
      {
        for(i <- filter_elem)
        {
          if(e(i.asInstanceOf[Tuple]).asInstanceOf[Boolean])
          {
            table_store = table_store :+ i.asInstanceOf[Tuple]
          }
        }
      }
    }
  }

  override def next(): Block = {
    var res : Block = null
    if(filter_ctr < table_store.size)
    {
      res = IndexedSeq[Tuple]()
      if((filter_ctr + blk_size).asInstanceOf[Int] > table_store.size)
      {
        blk_size = table_store.size - filter_ctr
      }
      for (i <- filter_ctr until (filter_ctr + blk_size).asInstanceOf[Int])
      {
        res = res :+ table_store(i)
      }
      filter_ctr += blk_size.asInstanceOf[Int]
    }
    res
  }

  override def close(): Unit = {
    input.close()
  }
}
