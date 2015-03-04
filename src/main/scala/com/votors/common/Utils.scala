package com.votors.common

/**
 * Created by chenzhiwei on 2/28/2015.
 */
object Utils extends java.io.Serializable{
  def string2Int(s: String, default: Int=0): Int = {
    try{
      s.toFloat.toInt
    }catch{
      case e: Exception => default
    }
  }
  //class TraceLevel extends Enumeration {}
  object Trace extends Enumeration {
    type TraceLevel = Value
    val DEBUG,INFO,WARN,ERROR=Value
    def trace(level: TraceLevel, x: Any) = println(x)
  }
}

class InterObj extends java.io.Serializable {
  var valueDouble: Double= 0
}