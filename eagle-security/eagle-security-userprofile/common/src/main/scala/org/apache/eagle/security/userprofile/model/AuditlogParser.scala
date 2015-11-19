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
package org.apache.eagle.security.userprofile.model

import org.apache.eagle.security.hdfs.{HDFSAuditLogObject, HDFSAuditLogParser}
import org.apache.eagle.security.userprofile.UserProfileUtils
import org.joda.time.Period

/**
 * {"getfileinfo", "open", "listStatus", "setTimes", "setPermission", "rename", "mkdirs", "create", "setReplication", "contentSummary", "delete", "setOwner", "fsck"}
 */
case class UserCommandActivity(timestamp:Long,user:String,cmd:String,periodWindowSeconds:Long)

/**
 * SELECT a.ugi, a.dt, a.hour, a.cmd, count(1)
 * from audit_log a
 * WHERE a.dt>='${STARTDATE}' AND a.dt<='${ENDDATE}'AND a.hour >= '${STARTTIME}' AND a.hour < '${ENDTIME}'
 * GROUP BY a.ugi,a.dt, a.hour, a.cmd;
 */
case class AuditLogTransformer(period:Period) extends Serializable {
  val parser = new HDFSAuditLogParser()
  // serialize periodInSeconds to avoid recreating Seconds object
  val periodInSeconds = period.toStandardSeconds

  def getObject(value: String) : Option[HDFSAuditLogObject] = {
    Option(parser.parse(value))
  }

  def transform(value: String) : Option[UserCommandActivity] = {
    Option(parser.parse(value)) match {
      case Some(entity) =>
        val timestamp = entity.timestamp
        val user = entity.user
        val cmd = entity.cmd
        val periodWindowSeconds = UserProfileUtils.formatSecondsByPeriod (timestamp / 1000, periodInSeconds)
        Some(UserCommandActivity (timestamp, user, cmd, periodWindowSeconds))
      case None => None
    }
  }
}