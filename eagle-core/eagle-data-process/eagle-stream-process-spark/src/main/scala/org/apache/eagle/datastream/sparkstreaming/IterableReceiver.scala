/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.apache.eagle.datastream.sparkstreaming

import org.apache.eagle.datastream.core.StreamInfo
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.LoggerFactory

class IterableReceiver (iterable: Iterable[Any],recycle:Boolean ) (implicit info:StreamInfo) extends Receiver[Any](StorageLevel.MEMORY_AND_DISK_2) with Logging{

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
    while(!isStopped){
      if(info.outKeyed) {
        store(_iterator.next().asInstanceOf[AnyRef])
      }else{
        store(_iterator.next().asInstanceOf[AnyRef])
      }
      if(!_iterator.hasNext && recycle){
        _iterator = iterable.iterator;
        Thread.sleep(5000)

      }
    }
  }

}
