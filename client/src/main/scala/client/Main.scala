package client

import io.circe.generic.auto._
import io.circe.syntax._
import model.{Page, Product, PurchaseEvent}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest

import java.util.UUID

object Main extends App {
  val kinesisClient = KinesisClient.create()

  for (_ <- 1 to 100) {
    val event = PurchaseEvent(
      UUID.randomUUID(),
      List(Product(12345, "Scala")),
      Some(Page("https://www.scala-lang.org/", "Scala"))
    )
    val serializedEvent = SdkBytes.fromUtf8String(event.asJson.toString())

    kinesisClient.putRecord(
      PutRecordRequest.builder()
        .streamName("events")
        .partitionKey(UUID.randomUUID().toString)
        .data(serializedEvent)
        .build()
    )
  }
}
