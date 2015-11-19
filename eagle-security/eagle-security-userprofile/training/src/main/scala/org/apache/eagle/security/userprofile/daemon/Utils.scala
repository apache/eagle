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

import java.util.regex.Pattern
import org.apache.eagle.common.DateTimeUtil
import org.apache.eagle.security.userprofile.UserProfileUtils
import org.joda.time.Period

import scala.collection.mutable

/**
 * @since  9/2/15
 */
object Utils {
  val PATH_TIME_FORMAT_PATTERN = Pattern.compile("\\$\\{([\\*\\/\\w\\-_\\s]+)\\}")

  /**
   * Example: hdfs:///logs/nn/auditlog/${yyyy-mm-dd}/hdfs-audit.log.${yyyy-mm-dd-hh}.gz
   *
   * @param path
   * @return
   */
  def formatPathWithMilliseconds(path:String)(milliseconds:Long):String = {
    val matcher = PATH_TIME_FORMAT_PATTERN.matcher(path)
    var result = path
    while(matcher.find()){
      val grouped = matcher.group()
      val format = grouped.substring(2,grouped.length()-1)
      val formatted = DateTimeUtil.format(milliseconds,format)
      result = result.replace(grouped,formatted)
    }
    result
  }

  /**
   * For example:
   *
   * current: 2015/09/02 10:12:32
   * granularity: 15m
   * duration: 1M // changing it to 1M interval of aggregation
   *
   * 1) flatten to duration: 2015/09/02 10:12:32 => 2015/09/02 10:00:00
   * 2) split by granularity: 2015/09/02 10:00:00 / 15m => [2015/09/02 10:00:00,2015/09/02 10:15:00,2015/09/02 10:30:00,2015/09/02 10:45:00]
   * 3) format file paths
   */
  def formatPathsInDuration(pathFormat:String,milliseconds:Long,duration:Period,granularity: Period = Period.parse("PT1M")): Seq[String] = {
    val durationIsSeconds = duration.toStandardSeconds
    val durationInMs = Int.int2long(durationIsSeconds.getSeconds) * 1000
    val granularityInMs = Int.int2long(granularity.toStandardSeconds.getSeconds) * 1000

    val flattenTimestamp =UserProfileUtils.formatSecondsByPeriod(milliseconds/1000,durationIsSeconds) * 1000
    val timeSplits:mutable.MutableList[Long] = new mutable.MutableList[Long]()
    var tmp = flattenTimestamp

    while(tmp - flattenTimestamp < durationInMs){
      timeSplits += tmp
      tmp = tmp + granularityInMs
    }

    timeSplits.map(formatPathWithMilliseconds(pathFormat))
  }
}