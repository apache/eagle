package org.apache.eagle.datastream

import org.apache.eagle.datastream.sparkstreaming.SparkStreamingExecutionEnvironment

/**
  * Created by zqin on 2016/1/26.
  */
object TestSparkStreamingDSL extends App{
  val env = ExecutionEnvironments.get[SparkStreamingExecutionEnvironment](args)
  val tuples = Seq(
    Entity("a", 1),
    Entity("a", 2),
    Entity("a", 3),
    Entity("b", 2),
    Entity("c", 3),
    Entity("d", 3)
  )

  env.from(tuples,recycle = true)
    .map(o => {o.inc += 2;o})
    .foreach(println)
  env.execute()
}
