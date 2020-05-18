package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected var i: Int = 0
  protected var container : IndexedSeq[Any] = _
  protected var col_row_iter: Int = 0
  protected var pax_page_num: Int = 0
  protected var row_bool : Boolean = false
  protected var col_bool : Boolean = false
  protected var pax_bool : Boolean = false
  protected var full_col: ColumnStore = _
  protected var pax_page: PAXStore = _
  protected var column_amount : Int = table.getRowType.getFieldCount

  override def execute(): IndexedSeq[Column] = {
    val store = tableToStore(table.unwrap(classOf[ScannableTable]))
    row_bool = store.isInstanceOf[RowStore]
    col_bool = store.isInstanceOf[ColumnStore]
    pax_bool = store.isInstanceOf[PAXStore]

    var res : IndexedSeq[Column] = IndexedSeq[Column]()
    if(row_bool)
    {
      if(store.getRowCount.asInstanceOf[Int] > 0)
      {
        for (row_iter <- 0 until store.getRowCount.asInstanceOf[Int])
        {
          container = store.asInstanceOf[RowStore].getRow(row_iter)
          res = res :+ container
        }
      }
      res = res.transpose
    }
    else if(col_bool)
    {
      if(store.getRowCount.asInstanceOf[Int] > 0)
      {
        full_col = store.asInstanceOf[ColumnStore]
        for (col_iter <- 0 until column_amount)
        {
          container = full_col.getColumn(col_iter)
          res = res :+ container
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
            res = res :+ container
          }
        }
        res = res.transpose
      }
    }
    res
  }
}
