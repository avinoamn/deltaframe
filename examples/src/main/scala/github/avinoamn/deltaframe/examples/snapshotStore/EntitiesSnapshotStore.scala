package github.avinoamn.deltaframe.examples.snapshotStore

import github.avinoamn.deltaframe.examples.models.Entity.Columns.{ageColName, idColName, nameColName}
import github.avinoamn.deltaframe.models.traits.SnapshotStore
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{PartitionSpec, Schema, SortOrder, Table, TableProperties}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.iceberg.types.Types
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.jdk.CollectionConverters._

case class EntitiesSnapshotStore(warehousePath: String,
                                 catalog: String,
                                 namespace: String,
                                 tableName: String,

                                 numBuckets: Int,

                                 // target file size for compaction (bytes)
                                 targetFileSizeBytes: Long = 128L * 1024L * 1024L, // 128 MB
                                )(implicit spark: SparkSession) extends SnapshotStore {

  private val fullTableIdent = s"$catalog.$namespace.$tableName"
  private val tableIdent = TableIdentifier.of(org.apache.iceberg.catalog.Namespace.of(namespace), tableName)

  // Hadoop catalog; adapt if you use HiveCatalog or Spark session catalog
  private val hadoopConf = new Configuration()
  private val icebergCatalog = new HadoopCatalog(hadoopConf, warehousePath)

  private val icebergFormat = "iceberg"

  // ---- Ensure the table exists and has the right schema/partition/sort ----
  private def getOrCreateTable(): Table = synchronized {
    if (icebergCatalog.tableExists(tableIdent)) {
      icebergCatalog.loadTable(tableIdent)
    } else {
      // Define schema
      val schema = new Schema(
        Types.NestedField.required(1, idColName, Types.StringType.get()),
        Types.NestedField.required(2, nameColName, Types.StringType.get()),
        Types.NestedField.required(3, ageColName, Types.IntegerType.get()),
      )

      val spec = PartitionSpec.builderFor(schema)
        .bucket(idColName, numBuckets)
        .build()

      val sortOrder = SortOrder.builderFor(schema).asc(idColName).build()

      val props = Map(
        TableProperties.FORMAT_VERSION -> "2",
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES -> targetFileSizeBytes.toString
      )

      icebergCatalog
        .buildTable(tableIdent, schema)
        .withPartitionSpec(spec)
        .withSortOrder(sortOrder)
        .withProperties(props.asJava)
        .create()
    }
  }

  private def getSnapshots(limit: Int): Array[Row] = {
    spark.sql(
      s"""
         |SELECT snapshot_id, committed_at
         |FROM $fullTableIdent.snapshots
         |ORDER BY committed_at DESC
         |LIMIT $limit
         |""".stripMargin
    ).collect()
  }

  def readLast(): DataFrame = {
    getOrCreateTable()
    spark.read
      .format(icebergFormat)
      .load(fullTableIdent)
  }

  def readPrev(): DataFrame = {
    getOrCreateTable()

    val snapshots = getSnapshots(2)

    if (snapshots.length > 1) {
      val prevSnapshotId = snapshots(1).getAs[Long]("snapshot_id")

      spark.read
        .format(icebergFormat)
        .option("snapshot-id", prevSnapshotId.toString)
        .load(fullTableIdent)
    } else {
      spark.emptyDataFrame
    }
  }

  override def writeLast(df: DataFrame): Unit = {
    getOrCreateTable()
    df.write
      .format(icebergFormat)
      .mode(SaveMode.Overwrite)
      .save(fullTableIdent)
  }

  override def compact(): Unit = {
    val table = getOrCreateTable()

    val now = System.currentTimeMillis()

    val sparkActions = SparkActions.get(spark)

    sparkActions.expireSnapshots(table).expireOlderThan(now).retainLast(2).execute()
    sparkActions.deleteOrphanFiles(table).olderThan(now).execute()
    spark.catalog.refreshTable(fullTableIdent)
  }
}
