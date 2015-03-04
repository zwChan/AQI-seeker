/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.votors.aqi

import java.util.Date
import org.apache.spark.{SparkConf, SparkContext}
import com.votors.common.Utils._
import com.votors.common.Utils.Trace._
import org.apache.spark.sql.{SQLContext,SchemaRDD}

//import org.apache.spark.sql.SQLContext


/*
  The main class of AQI-Seeker.
 */
class Aqi(sc: SparkContext, sqlContext: SQLContext, DataFilesOrPath: String, aqiFilesOrPath: String) extends java.io.Serializable {

  private def loader = new LoadData()
  private val originalRdd = loader.loadOrigina.cache()
  private val aqiRdd = loader.loadAqi.cache()
  var originDataTableName = "origin"
  var aqiDataTableName    = "aqi"
  import sqlContext.createSchemaRDD

  def createOriginTable() {
    trace(DEBUG,"originalRdd table take 10 items: " + originalRdd.take(10).mkString(","))
    originalRdd.registerTempTable(originDataTableName)
    originalRdd.printSchema()
  }
  def createAqiTable() {
    println("aqiRdd table take 10 items: " + aqiRdd.take(10).mkString(","))
    aqiRdd.registerTempTable(aqiDataTableName)
    aqiRdd.schema.fieldNames.foreach(println)
  }
  def sql(sql: String, limit: Int=1000) = {
    val sqlTemp = if (! sql.contains("limit") && limit > 0) sql + " limit " + limit else sql
    sqlContext.sql(sqlTemp)
  }

  class LoadData extends java.io.Serializable {
    def loadOrigina = {
      val DATA_POS_MAX=1007
      val DATA_POS_ID = 0
      val DATA_POS_DATE = 2
      val DATA_POS_HRMN = 3
      val DATA_POS_WINDDIR = 7
      val DATA_POS_WINDSPD = 10
      val DATA_POS_CLDHIGH = 12
      val DATA_POS_VISBY = 16
      val DATA_POS_TEMP = 20
      val DATA_POS_DEWPT = 22
      val DATA_POS_MARKS = DATA_POS_MAX-1

      val dataItems = sc.textFile(DataFilesOrPath, 3).flatMap(_.split("\n")).map(_.split(",")).filter(i => i.length >= DATA_POS_MAX && i(2).length == 8)
      //trace(DEBUG,"dataItems: " + dataItems.take(10).mkString(", "))
      val dataRdd = dataItems.map(i => {
        if (i.length > DATA_POS_MAX)
          OriginData(i(DATA_POS_ID), i(DATA_POS_DATE) + i(DATA_POS_HRMN),
            string2Int(i(DATA_POS_WINDDIR),Aqi.INVALID_NUM), string2Int(i(DATA_POS_WINDSPD),Aqi.INVALID_NUM),
            string2Int(i(DATA_POS_CLDHIGH),Aqi.INVALID_NUM), string2Int(i(DATA_POS_VISBY),Aqi.INVALID_NUM),
            string2Int(i(DATA_POS_TEMP),Aqi.INVALID_NUM), string2Int(i(DATA_POS_DEWPT),Aqi.INVALID_NUM),
            i(DATA_POS_MARKS).split(";")(0)).normalize()
        else
          OriginData(Aqi.INVALID_STR,Aqi.INVALID_STR,Aqi.INVALID_NUM,Aqi.INVALID_NUM,Aqi.INVALID_NUM,Aqi.INVALID_NUM,Aqi.INVALID_NUM,Aqi.INVALID_NUM,Aqi.INVALID_STR)
        })
      dataRdd
    }

    def loadAqi = {
      val aqiItems = sc.textFile(aqiFilesOrPath, 3).flatMap(_.split("\n")).map(_.split(",")).filter(_.length >= 10)
      //trace(DEBUG,"aqiItems: " + aqiItems.take(10).mkString(","))
      val aqiRDD = aqiItems.filter(i => i.length>7 && i(2).length>13).map(i => {
        AqiData(i(0),
          i(2).substring(0, 4) + i(2).substring(5, 7) + i(2).substring(8, 10) + i(2).substring(11, 13) + i(2).substring(14, 16),
          string2Int(i(7),Aqi.INVALID_NUM)).normalize()
      })
      aqiRDD
    }

  }

}

object Aqi {
  val INVALID_NUM = -999
  val INVALID_STR = INVALID_NUM.toString()
  def main(args: Array[String]) {
    if (args.length != 2){
      println("You should input option: original-file aqi-file!")
      return
    }

    val startTime = new Date()
    val sc = new SparkContext(new SparkConf().setAppName("AQI seeker"))
    val sqlContext = new SQLContext(sc)
    val aqi = new Aqi(sc, sqlContext,args(0), args(1))

    aqi.originDataTableName ="origin"
    aqi.aqiDataTableName    ="aqi"
    aqi.createAqiTable()
    aqi.createOriginTable()

    val combindRdd = aqi.sql("select origin.ts,aqi,temp,dewpt,visby,windSpd from origin, aqi where origin.ts=aqi.ts",-1)
    println(combindRdd.take(100).mkString("\t"))
    val cr = new Correlation(sc,sqlContext,combindRdd)
    val result = cr.corrs()
    println(result.mkString("\n"))
    println(result(2).xRdd.count())
    println(result(2))
    println(result(2).xRdd.collect().mkString("\t"))
    println(result(2).yRdd.collect().mkString("\t"))
    println(result(3).yRdd.collect().mkString("\t"))
    println(result(4).yRdd.collect().mkString("\t"))
    println(result(5).yRdd.collect().mkString("\t"))


    return
    trace(INFO,"select count(*) from origin limit 10")
    trace(INFO,aqi.sql("select * from origin").collect().mkString(","))
    trace(INFO,"select * from aqi limit 10")
    trace(INFO,aqi.sql("select count(*) from aqi").collect().mkString(","))

    println("*******result is ******************")
    //System.out.println("unsorted: " + dataItems.take(10).mkString(", "))
    val endTime = new Date()
    System.out.println("### used time: "+(endTime.getTime()-startTime.getTime())+" ###")
  }
}
