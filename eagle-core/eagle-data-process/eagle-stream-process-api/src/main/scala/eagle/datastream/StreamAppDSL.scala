package eagle.datastream

import backtype.storm.topology.base.BaseRichSpout
import com.typesafe.config.{ConfigValue, ConfigObject, ConfigValueFactory, Config}
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
    case TypeTag.Double => config.getDouble(key).asInstanceOf[T]
    case TypeTag.Long => config.getLong(key).asInstanceOf[T]
    case TypeTag.Int => config.getInt(key).asInstanceOf[T]
    case TypeTag.Byte => config.getBytes(key).asInstanceOf[T]
    case TypeTag.Boolean => config.getBoolean(key).asInstanceOf[T]
    case NUMBER_TYPE => config.getNumber(key).asInstanceOf[T]
    case OBJECT_TYPE => config.getObject(key).asInstanceOf[T]
    case VALUE_TYPE => config.getValue(key).asInstanceOf[T]
    case ANY_REF_TYPE => config.getAnyRef(key).asInstanceOf[T]

    case INT_LIST_TYPE => config.getIntList(key).asInstanceOf[T]
    case DOUBLE_LIST_TYPE => config.getDoubleList(key).asInstanceOf[T]
    case BOOL_LIST_TYPE => config.getBooleanList(key).asInstanceOf[T]
    case LONG_LIST_TYPE => config.getLongList(key).asInstanceOf[T]
    case _ => throw new UnsupportedOperationException(s"$tag is not supported yet")
  }

  val STRING_TYPE = typeOf[String]
  val NUMBER_TYPE = typeOf[Number]
  val INT_LIST_TYPE = typeOf[List[Int]]
  val BOOL_LIST_TYPE = typeOf[List[Boolean]]
  val DOUBLE_LIST_TYPE = typeOf[List[Double]]
  val LONG_LIST_TYPE = typeOf[List[Double]]
  val OBJECT_TYPE = typeOf[ConfigObject]
  val VALUE_TYPE = typeOf[ConfigValue]
  val ANY_REF_TYPE = typeOf[AnyRef]
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