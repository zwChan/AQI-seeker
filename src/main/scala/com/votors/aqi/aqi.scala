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
import org.apache.spark.SparkContext._

object aqi {
  def main(args: Array[String]) {

    val startTime = new Date()
    val sc = new SparkContext(new SparkConf().setAppName("aqi seeker"))
    val threshold = args(1).toInt
    val isCache = args(2).toInt
    // split each document into words
    val items = sc.textFile(args(0),3).flatMap(_.split(" "))


    println("*******result is ******************")
    System.out.println("unsorted: " + items.collect().mkString(", "))
    val endTime = new Date()
    System.out.println("used time: "+(endTime.getTime()-startTime.getTime()))
  }
}
