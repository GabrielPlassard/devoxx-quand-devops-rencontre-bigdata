package fr.devoxx.devops.logs.spark

import fr.devoxx.devops.logs.ApacheAccessLog
import org.apache.spark.rdd.RDD

/* Top 3 des familles user agents */
case class Spark3(rdd: RDD[String]) {

  def process = {
    rdd.map(ApacheAccessLog.parse)
      .map(l => (l.agentFamily, 1))
      .reduceByKey((a,b) => a + b)
      .top(3)(Ordering.by(_._2))
  }
}
