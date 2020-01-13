package demo

import software.amazon.awscdk
import software.amazon.awscdk.core.{RemovalPolicy, Stack}
import software.amazon.awscdk.services.glue.{Column, DataFormat, Database, InputFormat, OutputFormat, Schema, SerializationLibrary, Table}
import software.amazon.awscdk.services.iam.{Effect, PolicyDocument, PolicyStatement, Role, ServicePrincipal}
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream._
import software.amazon.awscdk.services.logs.{LogGroup, LogStream, RetentionDays}
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

    def createJsonToParquetFirehose(firehoseName: String,
                                    glueTable: Table,
                                    bucket: Bucket,
                                    s3Prefix: String = "data/"): CfnDeliveryStream = {
      val logGroup = LogGroup.Builder.create(stack, s"${firehoseName}_firehose_loggroup")
        .retention(RetentionDays.ONE_WEEK)
        .logGroupName(s"/aws/kinesisfirehose/$firehoseName")
        .removalPolicy(RemovalPolicy.DESTROY)
        .build()

      val logStream = LogStream.Builder.create(stack, s"${firehoseName}_firehose_logstream")
        .logGroup(logGroup)
        .logStreamName("S3Delivery")
        .removalPolicy(RemovalPolicy.DESTROY)
        .build()

      val role = Role.Builder.create(stack, s"${firehoseName}_firehose_role")
        .roleName(s"${firehoseName}_firehose_role")
        .assumedBy(ServicePrincipal.Builder.create("firehose.amazonaws.com")
          .conditions(Map("StringEquals" -> Map("sts:ExternalId" -> stack.getAccount).asJava.asInstanceOf[AnyRef]).asJava)
          .build()
        )
        .inlinePolicies(Map(
          "default" -> PolicyDocument.Builder.create()
            .statements(Seq(
              PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .resources(Seq(
                  bucket.getBucketArn,
                  s"${bucket.getBucketArn}/*"
                ).asJava)
                .actions(Seq(
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
                .resources(Seq("*").asJava)
                .actions(Seq(
                  "glue:GetTable",
                  "glue:GetTableVersion",
                  "glue:GetTableVersions"
                ).asJava)
                .build(),
              PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .resources(Seq(
                  s"arn:aws:logs:${stack.getRegion}:${stack.getAccount}:log-group:${logGroup.getLogGroupName}:log-stream:*"
                ).asJava)
                .actions(Seq("logs:PutLogEvents").asJava)
                .build()
            ).asJava)
            .build()
        ).asJava)
        .build()

      CfnDeliveryStream.Builder.create(stack, s"${firehoseName}_firehose")
        .deliveryStreamName(firehoseName)
        .extendedS3DestinationConfiguration(ExtendedS3DestinationConfigurationProperty.builder()
          .cloudWatchLoggingOptions(CloudWatchLoggingOptionsProperty.builder()
            .enabled(true)
            .logGroupName(logGroup.getLogGroupName)
            .logStreamName(logStream.getLogStreamName)
            .build()
          )
          .bucketArn(bucket.getBucketArn)
          .prefix(s"${s3Prefix}year=!{timestamp:YYYY}/month=!{timestamp:MM}/day=!{timestamp:dd}/")
          .errorOutputPrefix(s"error/!{firehose:error-output-type}/year=!{timestamp:YYYY}/month=!{timestamp:MM}/day=!{timestamp:dd}/")
          .roleArn(role.getRoleArn)
          .bufferingHints(BufferingHintsProperty.builder()
            .intervalInSeconds(60)
            .sizeInMBs(64)
            .build()
          )
          .compressionFormat("UNCOMPRESSED")
          .dataFormatConversionConfiguration(DataFormatConversionConfigurationProperty.builder()
            .enabled(true)
            .inputFormatConfiguration(InputFormatConfigurationProperty.builder()
              .deserializer(DeserializerProperty.builder().hiveJsonSerDe(HiveJsonSerDeProperty.builder().build()).build())
              .build()
            )
            .outputFormatConfiguration(OutputFormatConfigurationProperty.builder()
              .serializer(SerializerProperty.builder().parquetSerDe(ParquetSerDeProperty.builder().build()).build())
              .build()
            )
            .schemaConfiguration(SchemaConfigurationProperty.builder()
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
  }

  implicit class DatabaseOps(val database: Database) extends AnyVal {
    def createParquetTable(tableName: String,
                           schema: CompositeGlueSchema,
                           bucket: Bucket,
                           s3Prefix: String = "data/"): Table = {
      Table.Builder.create(database.getStack, s"${tableName}_table")
        .database(database)
        .tableName(tableName)
        .bucket(bucket)
        .s3Prefix(s3Prefix)
        .dataFormat(
          DataFormat.builder().inputFormat(InputFormat.PARQUET)
            .outputFormat(OutputFormat.PARQUET)
            .serializationLibrary(SerializationLibrary.PARQUET)
            .build()
        )
        .partitionKeys(Seq(
          Column.builder().name("year").`type`(Schema.STRING).build(),
          Column.builder().name("month").`type`(Schema.STRING).build(),
          Column.builder().name("day").`type`(Schema.STRING).build()
        ).asJava)
        .columns(schema.columns.asJava)
        .build()
    }
  }

}
