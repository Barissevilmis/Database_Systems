package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected val scannable : Store = tableToStore(table.unwrap(classOf[ScannableTable]))
  protected var row_iter : Int = 0
  protected var col_iter : Int = 0
  protected var col_row_iter: Int = 0
  protected var pax_iter : Int = 0
  protected var pax_page_num : Int = 0
  protected var row_bool : Boolean = false
  protected var col_bool : Boolean = false
  protected var pax_bool : Boolean = false
  protected var full_col : ColumnStore = _
  protected var pax_page : PAXStore = _
  protected var container : IndexedSeq[Any] = _
  protected var table_store : IndexedSeq[Tuple] = _
  protected var pax_transpose : IndexedSeq[IndexedSeq[Any]] = _
  protected var column_amount : Int = table.getRowType.getFieldCount

  override def open(): Unit = {
    row_bool = scannable.isInstanceOf[RowStore]
    col_bool = scannable.isInstanceOf[ColumnStore]
    pax_bool = scannable.isInstanceOf[PAXStore]
    table_store = IndexedSeq[Tuple]()
    if(scannable.getRowCount <= 0)
    {
      row_bool = false
      col_bool = false
      pax_bool = false
    }
    if(row_bool)
    {
      for(i <- 0 until scannable.getRowCount.asInstanceOf[Int])
      {
        table_store = table_store :+ scannable.asInstanceOf[RowStore].getRow(i)
      }
    }
    else if(col_bool)
    {
      full_col = scannable.asInstanceOf[ColumnStore]
      for(i <- 0 until column_amount)
      {
        table_store = table_store :+ full_col.getColumn(i)
      }
      table_store = table_store.transpose
    }
    else if(pax_bool)
    {
      pax_page = scannable.asInstanceOf[PAXStore]
      pax_page_num = scannable.getRowCount.asInstanceOf[Int] / pax_page.getPAXPage(0)(0).size
      if((scannable.getRowCount.asInstanceOf[Int] % pax_page.getPAXPage(0)(0).size) != 0)
      {
        pax_page_num += 1
      }
      for(i <- 0 until pax_page_num)
      {
        pax_transpose = pax_page.getPAXPage(i).transpose
        for(j <- pax_transpose.indices)
        {
          table_store = table_store :+ pax_transpose(j)
        }
      }
    }
  }

  override def next(): Tuple = {
    var res : Tuple = null
    if(row_bool)
    {
      if (row_iter < scannable.getRowCount)
        {
          res = IndexedSeq[Any]()
          res = table_store(row_iter)
          row_iter += 1
        }
    }
    else if(col_bool)
    {
      if(col_iter < scannable.getRowCount)
      {
        res = IndexedSeq[Any]()
        res = table_store(col_iter)
        col_iter += 1
      }
    }
    else if(pax_bool)
    {
      if(pax_iter < scannable.getRowCount)
      {
        res = IndexedSeq[Any]()
        res = table_store(pax_iter)
        pax_iter += 1
      }
    }
    res
  }



  override def close(): Unit = {}
}
