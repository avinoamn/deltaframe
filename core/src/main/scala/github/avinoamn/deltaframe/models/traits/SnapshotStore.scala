package github.avinoamn.deltaframe.models.traits

import org.apache.spark.sql.DataFrame

trait SnapshotStore {
  def readLast(): DataFrame

  def readPrev(): DataFrame

  def writeLast(df: DataFrame): Unit

  def compact(): Unit
}
