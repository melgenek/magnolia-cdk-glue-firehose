package demo.glue

import demo.glue.GlueSchema.Scalar
import magnolia1._
import software.amazon.awscdk.services.glue.{Column, Schema}

import java.util.UUID
import scala.language.experimental.macros
import scala.language.implicitConversions

trait GlueSchemaGenerator[T] {
  def apply(): GlueSchema
}

object GlueSchemaGenerator {
  type Typeclass[T] = GlueSchemaGenerator[T]

  def join[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] = () => {
    GlueSchema.Struct(
      caseClass.parameters
        .map { p =>
          Column.builder()
            .name(p.label)
            .`type`(p.typeclass.apply().glueType)
            .build()
        }
        .toList
    )
  }

  implicit def schema[T]: GlueSchemaGenerator[T] = macro Magnolia.gen[T]

  implicit def optionType[T](implicit schema: GlueSchemaGenerator[T]): GlueSchemaGenerator[Option[T]] =
    () => schema()

  implicit def listType[T](implicit schema: GlueSchemaGenerator[T]): GlueSchemaGenerator[List[T]] =
    () => GlueSchema.Scalar(Schema.array(schema().glueType))

  implicit val UUIDSchema: GlueSchemaGenerator[UUID] = () => Scalar(Schema.STRING)
  implicit val StringSchema: GlueSchemaGenerator[String] = () => Scalar(Schema.STRING)
  implicit val BooleanSchema: GlueSchemaGenerator[Boolean] = () => Scalar(Schema.BOOLEAN)
  implicit val LongSchema: GlueSchemaGenerator[Long] = () => Scalar(Schema.BIG_INT)
  implicit val IntSchema: GlueSchemaGenerator[Int] = () => Scalar(Schema.BIG_INT)
  implicit val DoubleSchema: GlueSchemaGenerator[Double] = () => Scalar(Schema.DOUBLE)
  implicit val FloatSchema: GlueSchemaGenerator[Float] = () => Scalar(Schema.DOUBLE)
}
