package org.apache.eagle.datastream.sparkstreaming


import org.apache.eagle.datastream.core.StreamInfo
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.LoggerFactory

/**
  * Created by zqin on 2016/1/25.
  */
class IterableReceiver(iterable: Iterable[Any],recycle:Boolean = true) (implicit info:StreamInfo) extends Receiver[Any](StorageLevel.MEMORY_AND_DISK_2) with Logging{

  val LOG = LoggerFactory.getLogger(classOf[IterableReceiver])
  var _iterator:Iterator[Any] = null
  def onStart(): Unit = {
    new Thread("Iterable Receiver") {
      _iterator = iterable.iterator
      override def run() { receive()}
    }.start()
  }

  def onStop(): Unit = {
  }

  private def receive(): Unit = {
    if(!isStopped && _iterator.hasNext){
      if(info.outKeyed) {
        store(_iterator.next().asInstanceOf[AnyRef])
      }else{
        store(_iterator.next().asInstanceOf[AnyRef])
      }
    }
    else if(!isStopped && !_iterator.hasNext){
      _iterator = iterable.iterator
    }
  }

}
