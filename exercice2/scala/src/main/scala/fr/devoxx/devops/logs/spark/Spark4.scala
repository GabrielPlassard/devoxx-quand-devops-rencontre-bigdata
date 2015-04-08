package fr.devoxx.devops.logs.spark

import fr.devoxx.devops.logs.ApacheAccessLog
import org.apache.spark.rdd.RDD

/* Top 3 des plages d'IP */
case class Spark4(rdd: RDD[String]) {

  def process = {
    rdd.map(ApacheAccessLog.parse)
      .map(l => (l.ipRange, 1))
      .reduceByKey((a,b) => a + b)
      .top(3)(Ordering.by(_._2))
  }
}
