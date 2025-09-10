package github.avinoamn.deltaframe

import github.avinoamn.deltaframe.models.traits.SnapshotStore
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

case class DeltaFrame(idColName: String, otherColNames: String*)(implicit spark: SparkSession) {

  case class Delta(newDelta: DataFrame, deletedDelta: DataFrame, updatedDelta: DataFrame)

  case class Batch(snapshotStore: SnapshotStore) {

    def run(newSnapshot: DataFrame): Delta = {
      snapshotStore.writeLast(newSnapshot)
      snapshotStore.compact()

      val lastSnapshot = snapshotStore.readLast()
      val prevSnapshot = snapshotStore.readPrev()

      if (prevSnapshot.isEmpty) {
        Delta(lastSnapshot, spark.emptyDataFrame, spark.emptyDataFrame)
      } else {
        DeltaFrame.this.run(lastSnapshot, prevSnapshot)
      }
    }
  }

  def run(newSnapshot: DataFrame, oldSnapshot: DataFrame): Delta = {
    val cachedNewSnapshot = newSnapshot.cache()
    val cachedOldSnapshot = oldSnapshot.cache()

    val newDiff = cachedNewSnapshot.join(cachedOldSnapshot.select(idColName), Seq(idColName), "left_anti")
    val deletedDiff = cachedOldSnapshot.join(cachedNewSnapshot.select(idColName), Seq(idColName), "left_anti")

    val updatedDiff = if (otherColNames.isEmpty) {
      spark.emptyDataFrame
    } else {
      cachedNewSnapshot.join(
        cachedOldSnapshot.select(idColName, otherColNames: _*),
        otherColNames.foldLeft(cachedNewSnapshot(idColName) === cachedOldSnapshot(idColName))((condition, colName) => {
          condition && cachedNewSnapshot(colName) =!= cachedOldSnapshot(colName)
        }),
        "inner"
      ).select(cachedNewSnapshot.columns.map(col): _*)
    }

    Delta(newDiff, deletedDiff, updatedDiff)
  }
}
