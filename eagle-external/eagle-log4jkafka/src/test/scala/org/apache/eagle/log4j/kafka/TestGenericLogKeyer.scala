/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.eagle.log4j.kafka

import java.util.Properties
import org.apache.eagle.log4j.kafka.hadoop.GenericLogKeyer
import org.scalatest.{FlatSpec, Matchers}


class TestGenericLogKeyer extends FlatSpec with Matchers  {

  val hdfsMsg = "2015-07-31 01:54:35,161 INFO FSNamesystem.audit: allowed=true ugi=root (auth:TOKEN) ip=/10.0.0.1 cmd=open src=/tmp/private dst=null perm=null"
  val props = new Properties()
  props.put("keyPattern", "ugi=(\\w+)[@\\s+]")
  props.put("keyPattern2", "user=(\\w+),\\s+")
  val test = new GenericLogKeyer(props)
  var keyVal = test.getKey(hdfsMsg)
  println(keyVal)

  val hbaseMsg = "2015-11-06 13:14:00,741 TRACE SecurityLogger.org.apache.hadoop.hbase.security.access.AccessController: Access allowed for user root; reason: All users allowed; remote address: /192.168.56.101; request: scan; context: (user=root, scope=hbase:meta, family=info, action=READ)"
  props.put("keyPattern", "user=(\\w+),\\s+")
  keyVal = test.getKey(hbaseMsg)
  println(keyVal)

  //props.put("keyPattern", "user=(\\w+),\\s+")
  val props1 = new Properties()
  val test1 = new GenericLogKeyer(props1)
  keyVal = test1.getKey(hbaseMsg)
  println(keyVal)


}
