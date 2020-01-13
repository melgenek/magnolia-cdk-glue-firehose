package demo

import demo.CdkOps.{AppOps, DatabaseOps, StackOps}
import software.amazon.awscdk

object Main extends App {

  case class Description(value: String)
  case class Recipient(email: String, phone: Option[String])
  case class Notification(id: Int, description: Option[Description], recipients: Set[Recipient])
  val schema = GlueSchema.schema[Notification]

  val app = new awscdk.core.App()
  val stack = app.createStack("melgenek-test-stack")
  val db = stack.createDatabase("melgenek_test")
  val bucket = stack.createBucket("melgenek-test-firehose")
  val table = db.createParquetTable("melgenek_test", schema, bucket)
  stack.createJsonToParquetFirehose("melgenek_test_firehose", table, bucket)

  app.synth()

}
