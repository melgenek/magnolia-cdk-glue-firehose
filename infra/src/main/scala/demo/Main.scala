package demo

import demo.CdkOps._
import demo.glue.GlueSchema
import model.PurchaseEvent
import software.amazon.awscdk

object Main extends App {
  val schema = GlueSchema.schema[PurchaseEvent]

  val app = new awscdk.core.App()
  val stack = app.createStack("demo")

  val bucket = stack.createBucket("demo-purchase-events")
  val kinesis = stack.createKinesisStream("events")

  val db = stack.createDatabase("demo")
  val table = db.createParquetTable("events", schema, bucket)

  stack.createJsonToParquetFirehose("events-to-s3", kinesis, table, bucket)

  app.synth()
}
