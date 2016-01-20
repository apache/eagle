package org.apache.eagle.stream.pipeline

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

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
class ConfigSpec extends FlatSpec with Matchers{
  "Config" should "be overrode correctly" in {
    val conf1 = ConfigFactory.parseString(
      """
        |value=1
      """.stripMargin)
    val conf2 = ConfigFactory.parseString(
      """
        |value=2
      """.stripMargin)
    val conf3 = conf1.withFallback(conf2)
    val conf4 = conf2.withFallback(conf1)
    conf3.getInt("value") should be(1)
    conf4.getInt("value") should be(2)
  }
}
