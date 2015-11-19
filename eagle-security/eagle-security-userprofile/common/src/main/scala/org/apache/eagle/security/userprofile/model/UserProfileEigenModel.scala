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

import org.apache.commons.math3.linear.{RealMatrix, RealVector}
import org.apache.eagle.ml.model.MLModelAPIEntity
import org.apache.eagle.security.userprofile.UserProfileConstants
import org.codehaus.jackson.map.annotate.JsonSerialize

/**
 * @since  7/22/15
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
case class UserProfileEigenModel(
  override val version:Long,
  override val site:String,
  override val user:String,
  uMatrix: RealMatrix,
  diagonalMatrix: RealMatrix,
  dimension: Int,
  minVector: RealVector,
  maxVector: RealVector,
  principalComponents: Array[RealVector],
  maximumL2Norm:RealVector,
  minimumL2Norm:RealVector,
  statistics:Array[UserCommandStatistics]
) extends UserProfileModel(site,user,UserProfileConstants.EIGEN_DECOMPOSITION_ALGORITHM,version) with EntityConversion[MLModelAPIEntity] {
  override def toEntity: MLModelAPIEntity = UserProfileEigenModelEntity.serializeModel(this)
  override def fromEntity(m: MLModelAPIEntity): Unit = UserProfileEigenModelEntity.deserializeModel(m)
}