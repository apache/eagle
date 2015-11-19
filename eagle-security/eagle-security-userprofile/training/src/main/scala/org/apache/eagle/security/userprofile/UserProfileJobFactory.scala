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
package org.apache.eagle.security.userprofile

import org.apache.eagle.security.userprofile.job.AuditLogTrainingSparkJob
import org.joda.time.Period

/**
* @since  7/21/15
*/
object UserProfileJobFactory {
  def AuditlogTrainingSparkJob(site:String=null,input:String=null, master:String = "local[1]", appName:String = UserProfileConstants.DEFAULT_TRAINING_APP_NAME, cmdTypes:Seq[String] = UserProfileConstants.DEFAULT_CMD_TYPES,period:Period)(f: AuditLogTrainingSparkJob => Unit = null) = {
    val app = new AuditLogTrainingSparkJob(site,input,master,appName,cmdTypes,period)
    if (f != null) f(app)
    app
  }
}