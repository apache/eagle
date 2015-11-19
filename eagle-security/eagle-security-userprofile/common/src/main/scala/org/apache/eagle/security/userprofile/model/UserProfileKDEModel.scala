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

import org.apache.eagle.ml.model.MLModelAPIEntity
import org.apache.eagle.security.userprofile.UserProfileConstants

/**
 * @since  7/22/15
 */
case class UserProfileKDEModel (
   override val version:Long,
   override val site:String,
   override val user:String,
   statistics: Array[UserCommandStatistics],
   minProbabilityEstimate:Double,
   maxProbabilityEstimate: Double,
   nintyFivePercentileEstimate: Double,
   medianProbabilityEstimate: Double
) extends UserProfileModel(site,user,UserProfileConstants.KDE_ALGORITHM,version ) with EntityConversion[MLModelAPIEntity]{
  override def toEntity: MLModelAPIEntity = UserProfileKDEModelEntity.serializeModel(this)
  override def fromEntity(m: MLModelAPIEntity): Unit = UserProfileKDEModelEntity.deserializeModel(m)
}