package fr.devoxx.devops.logs.spark

import fr.devoxx.devops.logs.ApacheAccessLog
import org.apache.spark.rdd.RDD

/* Répartition des codes http */
case class Spark2(rdd: RDD[String]) {

  def process = {
    rdd.map(ApacheAccessLog.parse)
        .map(l => (l.code, 1))
        .reduceByKey((a,b) => a + b)
        .collect
  }
}
