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
package org.apache.eagle.datastream

import org.apache.eagle.datastream.storm.StormWrapperUtils
import org.scalatest.{FlatSpec, Matchers}

class StormWrapperUtilsSpec extends FlatSpec with Matchers{
  import StormWrapperUtils._
  "StormWrapperUtils" should "convert Tuple{1,2,3,..} to java.util.List" in {
    val list1 = productAsJavaList(new Tuple1("a"))
    list1.size() should be(1)
    list1.get(0) should be("a")

    val list2 = productAsJavaList(new Tuple2("a","b"))
    list2.size() should be(2)
    list2.get(0) should be("a")
    list2.get(1) should be("b")

    val list3 = productAsJavaList(new Tuple3("a","b","c"))
    list3.size() should be(3)
    list3.get(0) should be("a")
    list3.get(1) should be("b")
    list3.get(2) should be("c")
  }
}