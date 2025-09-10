package github.avinoamn.deltaframe.examples

import github.avinoamn.deltaframe.DeltaFrame
import github.avinoamn.deltaframe.examples.models.Entity.Columns.idColName
import github.avinoamn.deltaframe.examples.models.Entity.Data.{constantEntities, entitiesForDeletion, newEntities}
import github.avinoamn.deltaframe.examples.snapshotStore.EntitiesSnapshotStore
import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS
import org.apache.spark.sql.SparkSession

object DeltaFrameBatchExample {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(getClass.getSimpleName.dropRight(1))
      .config("spark.executor.memory", "3g")
      .config("spark.driver.memory", "3g")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config(Map(
        (s"spark.sql.catalog.deltaframe", classOf[org.apache.iceberg.spark.SparkCatalog].getName),
        (s"spark.sql.catalog.deltaframe.type", "hadoop"),
        (s"spark.sql.catalog.deltaframe.warehouse", s"${System.getProperty("user.dir")}/outputData/iceberg-warehouse"),
        ("spark.sql.extensions", classOf[org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions].getName)))
      .getOrCreate()

    spark.conf.set(SHUFFLE_PARTITIONS.key, 4)

    val entitiesSnapshotStore = EntitiesSnapshotStore(
      warehousePath = s"${System.getProperty("user.dir")}/outputData/iceberg-warehouse",
      catalog = "deltaframe",
      namespace = "db",
      tableName = "entitiesSnapshot",

      numBuckets = 1
    )

    import spark.implicits._

    val deltaframeBatch = DeltaFrame(idColName).Batch(entitiesSnapshotStore)

    val delta1 = deltaframeBatch.run((constantEntities ++ entitiesForDeletion).toDF())
    delta1.newDelta.show(false)

    val delta2 = deltaframeBatch.run((constantEntities ++ newEntities).toDF())
    delta2.deletedDelta.show(false)
    delta2.newDelta.show(false)
  }
}
