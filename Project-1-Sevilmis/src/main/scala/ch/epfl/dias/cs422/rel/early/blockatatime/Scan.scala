package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected lazy val store: Store = tableToStore(table.unwrap(classOf[ScannableTable]))
  protected var row_bool : Boolean = false
  protected var col_bool : Boolean = false
  protected var pax_bool : Boolean = false
  protected var pax_page_num: Int = 0
  protected var container : Tuple = _
  protected var pax_iter : Int = 0
  protected var row_iter : Int = 0
  protected var col_iter : Int = 0
  protected var blk_size : Long = blockSize
  protected var full_col : ColumnStore = _
  protected var pax_page : PAXStore = _
  protected var table_store: IndexedSeq[Tuple] = _
  protected var pax_transpose : IndexedSeq[IndexedSeq[Any]] = _
  protected var column_amount : Int = table.getRowType.getFieldCount


  override def open(): Unit = {
    row_bool = store.isInstanceOf[RowStore]
    col_bool = store.isInstanceOf[ColumnStore]
    pax_bool = store.isInstanceOf[PAXStore]
    table_store = IndexedSeq[Tuple]()
    if(store.getRowCount <= 0)
    {
      row_bool = false
      col_bool = false
      pax_bool = false
    }
    if(row_bool)
    {
      for(i <- 0 until store.getRowCount.asInstanceOf[Int])
      {
        table_store = table_store :+ store.asInstanceOf[RowStore].getRow(i)
      }
    }
    else if(col_bool)
    {
      full_col = store.asInstanceOf[ColumnStore]
      for(i <- 0 until column_amount)
      {
        table_store = table_store :+ full_col.getColumn(i)
      }
      table_store = table_store.transpose
    }
    else if(pax_bool)
    {
      pax_page = store.asInstanceOf[PAXStore]
      pax_page_num = store.getRowCount.asInstanceOf[Int] / pax_page.getPAXPage(0)(0).size
      if((store.getRowCount.asInstanceOf[Int] % pax_page.getPAXPage(0)(0).size) != 0)
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

  override def next(): Block = {
    var res : Block = null
    if(row_bool)
    {
      if(row_iter < store.getRowCount)
      {
        res = IndexedSeq[Tuple]()
        if((row_iter + blk_size).asInstanceOf[Int] > store.getRowCount.asInstanceOf[Int])
        {
           blk_size = store.getRowCount.asInstanceOf[Int] - row_iter
        }
        for(i <- row_iter until (row_iter + blk_size).asInstanceOf[Int])
        {
          res = res :+ table_store(i)
        }
        row_iter += blk_size.asInstanceOf[Int]
      }
    }
    else if(col_bool)
    {
      if(col_iter < store.getRowCount)
      {
        res = IndexedSeq[Tuple]()
        if((col_iter + blk_size).asInstanceOf[Int] > store.getRowCount.asInstanceOf[Int])
        {
          blk_size = store.getRowCount.asInstanceOf[Int] - col_iter
        }
        for(i <- col_iter until (col_iter + blk_size).asInstanceOf[Int])
        {
          res = res :+ table_store(i)
        }
        col_iter += blk_size.asInstanceOf[Int]
      }
    }
    else if(pax_bool)
    {
      if(pax_iter < store.getRowCount)
      {
        res = IndexedSeq[Tuple]()
        if((pax_iter + blk_size).asInstanceOf[Int] > store.getRowCount.asInstanceOf[Int])
        {
          blk_size = store.getRowCount.asInstanceOf[Int] - pax_iter
        }
        for(i <- pax_iter until (pax_iter + blk_size).asInstanceOf[Int])
        {
          res = res :+ table_store(i)
        }
        pax_iter += blk_size.asInstanceOf[Int]
      }
    }
    res
  }

  override def close(): Unit = {
  }
}
