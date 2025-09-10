package github.avinoamn.deltaframe.examples

import github.avinoamn.deltaframe.DeltaFrame
import github.avinoamn.deltaframe.examples.models.Entity.Columns.idColName
import github.avinoamn.deltaframe.examples.models.Entity.Data.{constantEntities, entitiesForDeletion, newEntities}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS

object DeltaFrameExample {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(getClass.getSimpleName.dropRight(1))
      .config("spark.executor.memory", "3g")
      .config("spark.driver.memory", "3g")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .getOrCreate()

    spark.conf.set(SHUFFLE_PARTITIONS.key, 4)

    import spark.implicits._

    val deltaframe = DeltaFrame(idColName)

    val delta = deltaframe.run(
      (constantEntities ++ newEntities).toDF(),
      (constantEntities ++ entitiesForDeletion).toDF(),
    )

    delta.deletedDelta.show(false)
    delta.newDelta.show(false)
  }
}
