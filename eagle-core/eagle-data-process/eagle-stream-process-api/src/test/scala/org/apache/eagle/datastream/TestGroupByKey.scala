package org.apache.eagle.datastream

/**
 * @author Chen, Hao (hchen9@ebay.com)
 * @since  12/4/15
 */
object TestGroupByKey extends App{
  case class Obj(name:String,value:Double,var inc:Int=0)
  val env = ExecutionEnvironments.get[StormExecutionEnvironment](args)

  val seq = Seq(
    Obj("a",1),
    Obj("a",2),
    Obj("a",3),
    Obj("b",2),
    Obj("c",3),
    Obj("d",3)
  )

  env.fromCollection(seq)
    .map(o => {o.inc+=2;o})
    .filter(_.name == "b")
    .groupByKey(_.name)
    .foreach(println(_))
  env.execute()
}