package demo

import magnolia.{CaseClass, Magnolia}
import software.amazon.awscdk.services.glue.{Column, Schema, Type}

import scala.jdk.CollectionConverters._
import scala.language.experimental.macros
import scala.language.implicitConversions

trait GlueSchemaGenerator[T] {
  def apply(): GlueSchema
}

object GlueSchemaGenerator {
  type Typeclass[T] = GlueSchemaGenerator[T]

  def combine[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] = () => {
    CompositeGlueSchema(
      caseClass.parameters
        .map { p =>
          Column.builder()
            .name(p.label)
            .`type`(p.typeclass.apply().glueType)
            .build()
        }
    )
  }

  implicit def schema[T]: GlueSchemaGenerator[T] = macro Magnolia.gen[T]

  implicit def optionType[T](implicit schema: GlueSchemaGenerator[T]): GlueSchemaGenerator[Option[T]] =
    () => schema()

  implicit def seqType[T](implicit schema: GlueSchemaGenerator[T]): GlueSchemaGenerator[Seq[T]] =
    () => SimpleGlueSchema(Schema.array(schema().glueType))

  implicit def setType[T](implicit schema: GlueSchemaGenerator[T]): GlueSchemaGenerator[Set[T]] =
    () => SimpleGlueSchema(Schema.array(schema().glueType))

  implicit val StringSchema: GlueSchemaGenerator[String] = Schema.STRING
  implicit val BooleanSchema: GlueSchemaGenerator[Boolean] = Schema.BOOLEAN
  implicit val LongSchema: GlueSchemaGenerator[Long] = Schema.BIG_INT
  implicit val IntSchema: GlueSchemaGenerator[Int] = Schema.BIG_INT
  implicit val ShortSchema: GlueSchemaGenerator[Short] = Schema.INTEGER
  implicit val ByteSchema: GlueSchemaGenerator[Byte] = Schema.INTEGER
  implicit val CharSchema: GlueSchemaGenerator[Char] = Schema.doChar(1)
  implicit val DoubleSchema: GlueSchemaGenerator[Double] = Schema.DOUBLE
  implicit val FloatSchema: GlueSchemaGenerator[Float] = Schema.DOUBLE

  implicit def schemaFromType[T](glueType: Type): GlueSchemaGenerator[T] = () => SimpleGlueSchema(glueType)
}

trait GlueSchema {
  def glueType: Type
}

case class CompositeGlueSchema(columns: Seq[Column]) extends GlueSchema {
  override def glueType: Type = Schema.struct(columns.asJava)
}

case class SimpleGlueSchema(glueType: Type) extends GlueSchema

object GlueSchema {
  def schema[T: GlueSchemaGenerator]: CompositeGlueSchema = implicitly[GlueSchemaGenerator[T]].apply() match {
    case s: CompositeGlueSchema => s
    case _ => throw new RuntimeException("Use case classes as a source for table schema")
  }
}
