package fr.devoxx.devops.logs.spark

import fr.devoxx.devops.logs.ApacheAccessLog
import org.apache.spark.rdd.RDD

/* Les liens cassés */
case class Spark1(rdd: RDD[String]) {

  def process: Long = {
    rdd.map(ApacheAccessLog.parse)
      .filter(_.code == 404)
      .map(l => l.referer)
      .distinct
      .count
  }
}
