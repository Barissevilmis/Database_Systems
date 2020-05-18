package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.late.{LazyEvaluatorAccess, LazyOperator}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected var i: Int = 0
  protected var container : IndexedSeq[Any] = _
  protected var col_row_iter: Int = 0
  protected var pax_ctr: Int = 0
  protected var pax_page_num: Int = 0
  protected var row_bool : Boolean = false
  protected var col_bool : Boolean = false
  protected var pax_bool : Boolean = false
  protected var full_col : ColumnStore = _
  protected var pax_page : PAXStore = _
  protected var vid_prod : LazyOperator = _
  protected var table_store : IndexedSeq[Column] = _
  protected var column_amount : Int = table.getRowType.getFieldCount
  protected var ctr : Range = _

  def evaluateScan(mask : Int): Long => Any = (vid : Long) => (table_store(vid.asInstanceOf[Int])(mask))

  private lazy val evals_scan : LazyEvaluatorAccess =
  {
    var eval_res : LazyEvaluatorAccess = null
    var eval_list : List[Long => Any] = List[Long => Any]()
    for(i <- 0 until column_amount)
    {
      eval_list = eval_list :+ evaluateScan(i)
    }
    eval_res = new LazyEvaluatorAccess(eval_list)
    eval_res
  }
  override def evaluators(): LazyEvaluatorAccess = evals_scan

  override def execute(): IndexedSeq[Column] = {
    val store = tableToStore(table.unwrap(classOf[ScannableTable]))

    row_bool = store.isInstanceOf[RowStore]
    col_bool = store.isInstanceOf[ColumnStore]
    pax_bool = store.isInstanceOf[PAXStore]

    var res : IndexedSeq[Column] = IndexedSeq[Column]()
    table_store = IndexedSeq[Column]()
    if(row_bool)
    {
      if(store.getRowCount.asInstanceOf[Int] > 0)
      {
        ctr = 0 until store.getRowCount.asInstanceOf[Int]
        for (row_iter <- 0 until store.getRowCount.asInstanceOf[Int])
        {
          container = store.asInstanceOf[RowStore].getRow(row_iter)
          table_store = table_store :+ container
          res = res :+ IndexedSeq(ctr(row_iter).toLong)
        }
      }
    }
    else if(col_bool)
    {
      if(store.getRowCount.asInstanceOf[Int] > 0)
      {
        ctr = 0 until store.getRowCount.asInstanceOf[Int]
        full_col = store.asInstanceOf[ColumnStore]
        for (col_iter <- 0 until column_amount)
        {
          container = full_col.getColumn(col_iter)
          table_store = table_store :+ container
        }
        table_store = table_store.transpose
        for(i <- table_store.indices)
        {
          res = res :+ IndexedSeq(ctr(i).toLong)
        }
      }
    }
    else if(pax_bool)
    {
      if(store.getRowCount.asInstanceOf[Int] > 0)
      {
        pax_page = store.asInstanceOf[PAXStore]
        pax_page_num = store.getRowCount.asInstanceOf[Int] / pax_page.getPAXPage(0)(0).size
        if((store.getRowCount.asInstanceOf[Int] % pax_page.getPAXPage(0)(0).size) != 0)
        {
          pax_page_num += 1
        }
        ctr = 0 until store.getRowCount.asInstanceOf[Int]
        for (i <- 0 until pax_page_num)
        {
          val k: Int = 0
          for (j <- pax_page.getPAXPage(i)(k).indices)
          {
            container = IndexedSeq[Any]()
            for (k <- pax_page.getPAXPage(i).indices)
            {
              container = container :+ pax_page.getPAXPage(i)(k)(j)
            }
            table_store = table_store :+ container
            res = res :+ IndexedSeq(ctr(pax_ctr).toLong)
            pax_ctr += 1
          }
        }
      }
    }
    res
  }

}
