package demo

import demo.glue.GlueSchema
import software.amazon.awscdk
import software.amazon.awscdk.core.{RemovalPolicy, Stack}
import software.amazon.awscdk.services.glue._
import software.amazon.awscdk.services.iam._
import software.amazon.awscdk.services.kinesis
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream._
import software.amazon.awscdk.services.s3.Bucket

import scala.jdk.CollectionConverters._

object CdkOps {

  implicit class AppOps(val app: awscdk.core.App) {
    def createStack(stackName: String): Stack = {
      new Stack(app, stackName)
    }
  }

  implicit class StackOps(val stack: Stack) extends AnyVal {
    def createDatabase(databaseName: String): Database = {
      Database.Builder.create(stack, s"${databaseName}_database")
        .databaseName(databaseName)
        .locationUri(" ")
        .build()
    }

    def createBucket(bucketName: String): Bucket = {
      Bucket.Builder.create(stack, s"${bucketName}_bucket")
        .bucketName(bucketName)
        .removalPolicy(RemovalPolicy.DESTROY)
        .build()
    }

    def createKinesisStream(streamName: String): kinesis.Stream = {
      kinesis.Stream.Builder.create(stack, s"${streamName}_kinesis_stream")
        .streamName(streamName)
        .shardCount(1)
        .build()
    }

    def createJsonToParquetFirehose(firehoseName: String,
                                    kinesisStream: kinesis.Stream,
                                    glueTable: Table,
                                    bucket: Bucket): CfnDeliveryStream = {
      val role = firehoseIamRole(firehoseName, kinesisStream, glueTable, bucket)

      CfnDeliveryStream.Builder.create(stack, s"${firehoseName}_firehose")
        .deliveryStreamName(firehoseName)
        .deliveryStreamType("KinesisStreamAsSource")
        .kinesisStreamSourceConfiguration(
          KinesisStreamSourceConfigurationProperty.builder()
            .kinesisStreamArn(kinesisStream.getStreamArn)
            .roleArn(role.getRoleArn)
            .build()
        )
        .extendedS3DestinationConfiguration(
          ExtendedS3DestinationConfigurationProperty.builder()
            .cloudWatchLoggingOptions(DisabledCloudwatch)
            .bucketArn(bucket.getBucketArn)
            .prefix(s"data/date=!{timestamp:YYYY}-!{timestamp:MM}-!{timestamp:dd}/")
            .errorOutputPrefix(s"error/!{firehose:error-output-type}/date=!{timestamp:YYYY}-!{timestamp:MM}-!{timestamp:dd}/")
            .roleArn(role.getRoleArn)
            .bufferingHints(
              BufferingHintsProperty.builder()
                .intervalInSeconds(60)
                .sizeInMBs(64)
                .build()
            )
            .compressionFormat("UNCOMPRESSED")
            .dataFormatConversionConfiguration(
              DataFormatConversionConfigurationProperty.builder()
                .enabled(true)
                .inputFormatConfiguration(JsonSerDe)
                .outputFormatConfiguration(ParquetSerDe)
                .schemaConfiguration(
                  SchemaConfigurationProperty.builder()
                    .roleArn(role.getRoleArn)
                    .catalogId(glueTable.getDatabase.getCatalogId)
                    .databaseName(glueTable.getDatabase.getDatabaseName)
                    .tableName(glueTable.getTableName)
                    .region(stack.getRegion)
                    .versionId("LATEST")
                    .build()
                ).build()
            ).build()
        ).build()
    }

    private def firehoseIamRole(firehoseName: String,
                                kinesisStream: kinesis.Stream,
                                glueTable: Table,
                                bucket: Bucket) = {
      Role.Builder.create(stack, s"${firehoseName}_firehose_role")
        .roleName(firehoseName)
        .assumedBy(
          ServicePrincipal.Builder.create("firehose.amazonaws.com")
            .conditions(Map("StringEquals" -> Map("sts:ExternalId" -> stack.getAccount).asJava.asInstanceOf[AnyRef]).asJava)
            .build()
        )
        .inlinePolicies(Map(
          "default" -> PolicyDocument.Builder.create()
            .statements(List(
              PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .resources(List(kinesisStream.getStreamArn).asJava)
                .actions(List(
                  "kinesis:DescribeStream",
                  "kinesis:GetShardIterator",
                  "kinesis:GetRecords",
                  "kinesis:ListShards"
                ).asJava)
                .build(),
              PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .resources(List(
                  bucket.getBucketArn,
                  s"${bucket.getBucketArn}/*"
                ).asJava)
                .actions(List(
                  "s3:AbortMultipartUpload",
                  "s3:GetBucketLocation",
                  "s3:GetObject",
                  "s3:ListBucket",
                  "s3:ListBucketMultipartUploads",
                  "s3:PutObject"
                ).asJava)
                .build(),
              PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .resources(List("*").asJava)
                .actions(List(
                  "glue:GetTable",
                  "glue:GetTableVersion",
                  "glue:GetTableVersions"
                ).asJava)
                .build()
            ).asJava)
            .build()
        ).asJava)
        .build()
    }
  }

  implicit class DatabaseOps(val database: Database) extends AnyVal {
    def createParquetTable(tableName: String,
                           schema: GlueSchema.Struct,
                           bucket: Bucket): Table = {
      Table.Builder.create(database.getStack, s"${tableName}_table")
        .database(database)
        .tableName(tableName)
        .bucket(bucket)
        .s3Prefix("data/")
        .dataFormat(DataFormat.PARQUET)
        .partitionKeys(List(
          Column.builder().name("date").`type`(Schema.DATE).build()
        ).asJava)
        .columns(schema.columns.asJava)
        .build()
    }
  }

  private final val JsonSerDe = InputFormatConfigurationProperty.builder()
    .deserializer(DeserializerProperty.builder().hiveJsonSerDe(HiveJsonSerDeProperty.builder().build()).build())
    .build()

  private final val ParquetSerDe = OutputFormatConfigurationProperty.builder()
    .serializer(SerializerProperty.builder().parquetSerDe(ParquetSerDeProperty.builder().build()).build())
    .build()

  private final val DisabledCloudwatch = CloudWatchLoggingOptionsProperty.builder()
    .enabled(false)
    .build()
}
