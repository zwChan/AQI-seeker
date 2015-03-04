package com.votors.aqi

/**
 * Created by chenzhiwei on 3/2/2015.
 */

import com.votors.common.InterObj
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{SQLContext,SchemaRDD}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import java.util.Date
import com.votors.common.Utils._
import com.votors.common.Utils.Trace._
import com.votors.aqi.Aqi._

case class crTable(x: String, y: String, cr: Double, flag: Int, xRdd: RDD[Double], yRdd: RDD[Double]) extends java.io.Serializable {
  override def toString = f"${x} ${y} ${cr} ${flag}"
}

class Correlation(@transient sc: SparkContext, @transient sqlContext: SQLContext, schemaRdd: SchemaRDD)  extends java.io.Serializable {
  def corrs(): Seq[crTable] = {
    val needCorrField: String = ";temp;dewpt;visby;cloudHigh;windSpd;windDir;"
    val fields = schemaRdd.schema.fieldNames.toArray
    val fieldNames = sc.broadcast(fields)
    schemaRdd.printSchema()
    val aqiIndex = schemaRdd.schema.fieldNames.indexOf("aqi")
    val interObj1 = new InterObj()
    interObj1.valueDouble = INVALID_NUM
    val aqiRdd = schemaRdd.map(r => {
      val item = r(aqiIndex).toString.toDouble
      val newItem = if (item == INVALID_NUM) {
        if (interObj1.valueDouble != INVALID_NUM)
          interObj1.valueDouble
        else
          0
      } else {
        if (interObj1.valueDouble == INVALID_NUM)
          interObj1.valueDouble = item
        else
          interObj1.valueDouble = interObj1.valueDouble*2/3 + item * 1 / 3
        item
      }
      newItem
    })
    val interObj = new InterObj()
    interObj.valueDouble = INVALID_NUM
    val corrTable = fieldNames.value.map(f => {
      val index = fieldNames.value.indexOf(f)
      val corr =
        if (needCorrField.contains(";" + f + ";")) {
          val fieldRdd = schemaRdd.map(r => {
            var flag = 0
            val fieldIndex = try{fieldNames.value.indexOf(f)}catch {case _ =>flag += 1;-1}
            if (flag > 0) {
              0.0
            } else {
              val item = r(fieldIndex).toString.toDouble
              val newItem = if (item == INVALID_NUM) {
                if (interObj.valueDouble != INVALID_NUM)
                  interObj.valueDouble
                else
                  0
              } else {
                if (interObj.valueDouble == INVALID_NUM)
                  interObj.valueDouble = item
                else
                  interObj.valueDouble = interObj.valueDouble*2/3 + item * 1 / 3
                item
              }

              newItem
            }

          })
          val cr = Statistics.corr(aqiRdd, fieldRdd)
          crTable("aqi",f,cr,0,aqiRdd,fieldRdd)
        } else {
          crTable("aqi", f, 0,1,null,null)
        }
      corr
      })
    corrTable
  }
}
