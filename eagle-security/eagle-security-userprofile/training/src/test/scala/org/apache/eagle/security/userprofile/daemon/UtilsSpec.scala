/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.security.userprofile.daemon

import org.apache.eagle.common.DateTimeUtil
import org.joda.time.Period
import org.scalatest._

/**
 * @since  9/2/15
 */
class UtilsSpec extends FlatSpec with Matchers{
  import org.apache.eagle.security.userprofile.daemon.Utils._

  it should "formatPathWithMilliseconds " in {
    formatPathWithMilliseconds("/tmp/path/to/log.${yyyy-MM-dd-hh}.gz")(DateTimeUtil.humanDateToMilliseconds("2015-09-02 10:32:17,000")) should be ("/tmp/path/to/log.2015-09-02-10.gz")
    formatPathWithMilliseconds("/tmp/path/to/${yyyy-MM-dd}/*.tar.gz")(DateTimeUtil.humanDateToMilliseconds("2015-09-02 10:32:17,000")) should be ("/tmp/path/to/2015-09-02/*.tar.gz")
    formatPathWithMilliseconds("/tmp/path/to/${yyyy-MM-dd}/${yyyy-MM-dd-hh}.tar.gz")(DateTimeUtil.humanDateToMilliseconds("2015-09-02 10:32:17,000")) should be ("/tmp/path/to/2015-09-02/2015-09-02-10.tar.gz")
  }

  it should "formatPathsInDuration" in {
    val pathes:Seq[String] = formatPathsInDuration("/tmp/path/to/log.${yyyy-MM-dd-hh}.gz",DateTimeUtil.humanDateToMilliseconds("2015-09-02 10:32:17,000"),Period.parse("P30D"))
    //pathes.length should be (30 * 24)
  }
}