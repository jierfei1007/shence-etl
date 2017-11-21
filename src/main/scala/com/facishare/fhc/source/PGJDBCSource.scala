package com.facishare.fhc.source

import java.sql.{Connection, DriverManager, ResultSetMetaData, SQLException}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by jief on
  * 2017/11/16.
  */
object PGJDBCSource {

  /**
    * 根据pg sql 查询返回dataframe
    * @param sqlContext
    * @param userName
    * @param pwd
    * @param url
    * @param dbName
    * @param sql
    * @return
    */
    def getPGSQLDF(sqlContext:SQLContext,userName:String,pwd:String,url:String,dbName:String,sql:String):DataFrame={
      val opt = Map(
        "url" -> s"jdbc:postgresql://$url/$dbName?useUnicode=true&characterEncoding=UTF-8",
        "driver" -> "org.postgresql.Driver",
        "user" -> userName,
        "password" -> pwd,
        "dbtable" -> s" ($sql) as result "
      )
      println(opt.get("dbtable"))
      sqlContext.read.format("jdbc").options(opt).load.toDF
    }

    def printPGSQLScheme(userName:String,pwd:String,url:String,dbName:String,sql:String)={
      val conn=DriverManager.getConnection(s"jdbc:postgresql://$url/$dbName?useUnicode=true&characterEncoding=UTF-8", userName, pwd)
      try {
        val statement = conn.prepareStatement(s"SELECT * FROM ($sql) as request WHERE 1=0")
        try {
          val rs = statement.executeQuery()
          try {
            val rsmd = rs.getMetaData
            val ncols = rsmd.getColumnCount
            var i = 0
            while (i < ncols) {
              val columnName = rsmd.getColumnLabel(i + 1)
              val dataType = rsmd.getColumnType(i + 1)
              val typeName = rsmd.getColumnTypeName(i + 1)
              val fieldSize = rsmd.getPrecision(i + 1)
              val fieldScale = rsmd.getScale(i + 1)
              val isSigned = rsmd.isSigned(i + 1)
              val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
              val metadata = new MetadataBuilder().putString("name", columnName)
//              val columnType =
//                dialect.getCatalystType(dataType, typeName, fieldSize, metadata).getOrElse(
//                  getCatalystType(dataType, fieldSize, fieldScale, isSigned))
//              fields(i) = StructField(columnName, columnType, nullable, metadata.build())
              println("columnName="+columnName+",dataType="+dataType+",typeName="+typeName+",isSigned="+isSigned)
              getCatalystType(dataType, fieldSize, fieldScale, isSigned)
              i = i + 1
            }
          } finally {
            rs.close()
          }
        } finally {
          statement.close()
        }
      } finally {
        conn.close()
      }
    }

  private def getCatalystType(
                               sqlType: Int,
                               precision: Int,
                               scale: Int,
                               signed: Boolean): DataType = {
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY         => null
      case java.sql.Types.BIGINT        => if (signed) { LongType } else { DecimalType(20,0) }
      case java.sql.Types.BINARY        => BinaryType
      case java.sql.Types.BIT           => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB          => BinaryType
      case java.sql.Types.BOOLEAN       => BooleanType
      case java.sql.Types.CHAR          => StringType
      case java.sql.Types.CLOB          => StringType
      case java.sql.Types.DATALINK      => null
      case java.sql.Types.DATE          => DateType
//      case java.sql.Types.DECIMAL
//        if precision != 0 || scale != 0 => DecimalType.bounded(precision, scale)
      case java.sql.Types.DECIMAL       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT      => null
      case java.sql.Types.DOUBLE        => DoubleType
      case java.sql.Types.FLOAT         => FloatType
      case java.sql.Types.INTEGER       => if (signed) { IntegerType } else { LongType }
      case java.sql.Types.JAVA_OBJECT   => null
      case java.sql.Types.LONGNVARCHAR  => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR   => StringType
      case java.sql.Types.NCHAR         => StringType
      case java.sql.Types.NCLOB         => StringType
      case java.sql.Types.NULL          => null
//      case java.sql.Types.NUMERIC
//        if precision != 0 || scale != 0 => DecimalType.bounded(precision, scale)
      case java.sql.Types.NUMERIC       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR      => StringType
      case java.sql.Types.OTHER         => null
      case java.sql.Types.REAL          => DoubleType
      case java.sql.Types.REF           => StringType
      case java.sql.Types.ROWID         => LongType
      case java.sql.Types.SMALLINT      => IntegerType
      case java.sql.Types.SQLXML        => StringType
      case java.sql.Types.STRUCT        => StringType
      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIMESTAMP     => TimestampType
      case java.sql.Types.TINYINT       => IntegerType
      case java.sql.Types.VARBINARY     => BinaryType
      case java.sql.Types.VARCHAR       => StringType
      case _                            => null
      // scalastyle:on
    }
    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }
}
