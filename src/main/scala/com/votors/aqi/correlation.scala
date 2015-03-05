package com.votors.aqi

/**
 * Created by chenzhiwei on 3/2/2015.
 */

import com.votors.common.{InterObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{SQLContext,SchemaRDD}
import org.apache.spark.mllib.linalg.{_}
import org.apache.spark.mllib.stat.Statistics
import java.util.Date
import com.votors.common.Utils._
import com.votors.common.Utils.Trace._
import com.votors.aqi.Aqi._

case class CrTable(x: String, y: String, cr: Double, flag: Int, xRdd: RDD[Double], yRdd: RDD[Double]) extends java.io.Serializable {
  override def toString = f"${x} ${y} ${cr}%.2f ${flag}"
}

/**
 * Correlation class is supposed to find the most relative feature to 'aqi'. It will evaluate the Pearson correlation
 * between 'aqi' to other fields.
 *
 * @param sc the sparkContext
 * @param sqlContext
 * @param schemaRdd
 */
class Correlation(@transient sc: SparkContext, @transient sqlContext: SQLContext, schemaRdd: SchemaRDD)  extends java.io.Serializable {
  // Create ts filed RDD
  val fields = schemaRdd.schema.fieldNames.toArray
  val fieldNames = sc.broadcast(fields)
  val tsIndex = fieldNames.value.indexOf("ts")
  val needCorrFieldDefault = "aqi"::"temp"::"dewpt"::"visby"::"cloudHigh"::"windSpd"::"windDir"::Nil

  val tsRdd = schemaRdd.map(r => {
    val tsIndex = fieldNames.value.indexOf("ts")
    r(tsIndex).toString
  })

  def corrs(mainField: String="aqi", someFiles: Seq[String]=Nil): Seq[CrTable] = {
    val needCorrField = if (someFiles.length>0) someFiles else needCorrFieldDefault
    trace(INFO,"Current schemaRDD schema is: ")
    trace(INFO,schemaRdd.schemaString)

    def timeFilter(t: String, start: Date, end: Date): Boolean = {
      val d = str2Date(t)
      d.getTime >= start.getTime && d.getTime < end.getTime
    }

    def timeOffset(offsetOnHour: Int): Date = {
      val row = schemaRdd.take(1)(0)
      val number = schemaRdd.count()
      val start = str2Date(row(tsIndex).asInstanceOf[String])
      trace(INFO, "The first time is: " + row(tsIndex))
      new Date(start.getTime + 3600*1000*offsetOnHour)
    }

    val mainIndex = schemaRdd.schema.fieldNames.indexOf(mainField)
    if (mainIndex == -1) {
      trace(ERROR, "mainFile not found")
      return null
    }
    // Create main filed RDD
    val interObjMain = new InterObject{}
    val aqiRdd = schemaRdd.map(r => {
      val item = r(mainIndex).toString.toDouble
      item
      //fixInvalid(item, INVALID_NUM,interObjMain,0,0)
    })


    val interObjOther = new InterObject()
    val corrTables = fieldNames.value.map(f => {
      val index = fieldNames.value.indexOf(f)
      assert(index != -1)
      val corrTable =
        if (needCorrField.contains(f)) {
          val fieldRdd = schemaRdd.map(r => {
              val item = r(index).toString.toDouble
              //fixInvalid(item, INVALID_NUM, interObjOther, 0,0)
              item
          })
          val cr = Statistics.corr(aqiRdd, fieldRdd)
          CrTable("aqi",f,cr,0,aqiRdd,fieldRdd)
        } else {
          CrTable("aqi", f, 0,1,null,null)
        }
      corrTable
    })
    corrTables
  }
  def corrsOfOffset(crTable: Seq[CrTable], offsetOnHour: Int=0): Seq[CrTable] = {
    def timeFilter(t: Long, start: Long): Boolean = {
      t >= start
    }

    val firstTime = {
      val t = tsRdd.take(1)(0)
      //val number = schemaRdd.count()
      //val index = fields.indexOf("ts")
      val start = str2Date(t)
      trace(INFO, "The first time is: " + t)
      start.getTime
    }
    val counter = tsRdd.count()
    val counterLeft = tsRdd.filter(t => timeFilter(str2Date(t).getTime, firstTime+3600*1000*offsetOnHour)).count()

    crTable.filter(_.flag == 0).map(crTbl => {
      val mainRdd = crTbl.xRdd.zipWithIndex().filter(_._2 >= counterLeft).map(_._1).repartition(10)
      val otherRdd = crTbl.yRdd.zipWithIndex().filter(_._2 >= counter - counterLeft).map(_._1).repartition(10)
      val cr = Statistics.corr(mainRdd, otherRdd)
      CrTable("aqi",crTbl.y+"-"+offsetOnHour,cr,0,null,null)
    })
  }

  /**
   * Transform the SchemaRDD to VectorRDD, and evaluate the correlation
   */
  def corrAll(someFiles: Seq[String]=Nil): Seq[CrTable] = {
    val needCorrField = if (someFiles.length>0) someFiles else needCorrFieldDefault
    trace(INFO,"Current schemaRDD schema is: ")
    trace(INFO,schemaRdd.schemaString)
    val veterRdd = schemaRdd.map(r => {
      val subRow = needCorrField.map(f =>{
        val index = fieldNames.value.indexOf(f)
        assert(index != -1, f"The ${f} is not found in ${fieldNames.value}")
        r(index).toString.toDouble
      })
      new DenseVector(subRow.toArray)
    })
    val cr = Statistics.corr(veterRdd.asInstanceOf[RDD[Vector]])
    trace(DEBUG,"All field correlation is:")
    trace(DEBUG,cr.toArray.mkString("\t"))
    val crArray = cr.toArray
    val aqiIndex = needCorrField.indexOf("aqi")
    assert(aqiIndex != -1, f"The 'aqi' is not found in needCorrField")
    val result = Range(0,cr.numCols).map(col => {
      CrTable("aqi",needCorrField(col),crArray(aqiIndex*cr.numCols + col),0,null,null)
    })
    result
  }
}
