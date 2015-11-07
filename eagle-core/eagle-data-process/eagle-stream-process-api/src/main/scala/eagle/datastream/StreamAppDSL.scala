package eagle.datastream

import backtype.storm.topology.base.BaseRichSpout
import com.typesafe.config.{ConfigValueFactory, Config}
import eagle.dataproc.impl.storm.AbstractStormSpoutProvider
import eagle.dataproc.util.ConfigOptionParser

import scala.reflect.runtime.universe._

/**
 * @since  11/6/15
 */
trait ConfigContext{
  def set(config:Config)
  def config:Config

  def set[T<:AnyRef](key:String,value:T): Unit = {
    set(config.withValue(key,ConfigValueFactory.fromAnyRef(value)))
  }

  /**
   *
   * @param key config key
   * @param default default value
   * @tparam T return type
   * @return
   */
  def get[T](key:String,default:T=null):T = {
    if(config.hasPath(key)) {
      get(key)
    } else default
  }

  def get[T](key:String)(implicit tag: TypeTag[T]):T = tag.tpe match {
    case STRING_TYPE => config.getString(key).asInstanceOf[T]
    case _ => throw new UnsupportedOperationException(s"$tag is not supported yet")
  }

  val STRING_TYPE = typeOf[String]
}

/**
 * Stream APP DSL
 * @tparam E
 */
trait StreamApp[+E<:ExecutionEnvironment] extends App with ConfigContext{
  var _executed = false
  var _config:Config = null

  override def config:Config = {
    if(_config == null) _config = new ConfigOptionParser().load(args)
    _config
  }

  override def set(config:Config) = _config = config

  def env:E
  def execute() {
    env.execute()
    _executed = true
  }

  override def main(args: Array[String]): Unit = {
    super.main(args)
    if(!_executed) execute()
  }
}

trait StormStreamApp extends StreamApp[StormExecutionEnvironment]{
  private val _env:StormExecutionEnvironment = ExecutionEnvironmentFactory.getStorm(config)
  def source(sourceProvider: AbstractStormSpoutProvider) = {
    val spout = sourceProvider.getSpout(config)
    _env.newSource(spout)
  }

  def source(spout:BaseRichSpout) = _env.newSource(spout)

  override def env = _env
}