package org.apache.eagle.datastream

/**
 * @since  12/4/15
 */
case class Entity(name:String,value:Double,var inc:Int=0)

object TestIterableWithGroupBy extends App {
  val env = ExecutionEnvironments.get[StormExecutionEnvironment](args)
  val tuples = Seq(
    Entity("a", 1),
    Entity("a", 2),
    Entity("a", 3),
    Entity("b", 2),
    Entity("c", 3),
    Entity("d", 3)
  )
  env.from(tuples)
    .map(o => {o.inc += 2;o})
    .filter(_.name != "b")
    .groupByKey(_.name)
    .foreach(println)
  env.execute()
}

object TestIterableWithGroupByCircularly extends App{
  val env = ExecutionEnvironments.get[StormExecutionEnvironment](args)
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
    .groupByKey(_.name)
    .foreach(println)
  env.execute()
}