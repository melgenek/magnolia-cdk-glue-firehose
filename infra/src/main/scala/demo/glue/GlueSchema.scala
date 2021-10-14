package demo.glue

import software.amazon.awscdk.services.glue.{Column, Schema, Type}

import scala.jdk.CollectionConverters._

sealed trait GlueSchema {
  def glueType: Type
}

object GlueSchema {

  final case class Scalar(glueType: Type) extends GlueSchema

  final case class Struct(columns: List[Column]) extends GlueSchema {
    override def glueType: Type = Schema.struct(columns.asJava)
  }

  def schema[T: GlueSchemaGenerator]: Struct = implicitly[GlueSchemaGenerator[T]].apply() match {
    case s: Struct => s
    case _ => throw new RuntimeException("Use case classes as a source for table schema")
  }

}
