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
package org.apache.eagle.security.userprofile.sink

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.eagle.security.userprofile.model.{EntityConversion, UserProfileContext, UserProfileModel}
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity
import org.apache.eagle.service.client.IEagleServiceClient
import org.apache.eagle.service.client.impl.EagleServiceClientImpl
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

trait UserProfileModelRDDSink extends Serializable{
  def persist(model: RDD[UserProfileModel],context: UserProfileContext)
}

abstract class UserProfileModelSink extends UserProfileModelRDDSink{
  def persist(model: UserProfileModel)
  override def persist(model: RDD[UserProfileModel],context:UserProfileContext): Unit = {
    model.foreach(persist)
  }
}

case class UserProfileHDFSSink(path:String) extends UserProfileModelRDDSink with Logging{
  override def persist(model: RDD[UserProfileModel],ctx: UserProfileContext): Unit = {
    val finalPath = if(path.endsWith("/")) String.format("%s%s",path,ctx.algrithm) else String.format("%s/%s",path,ctx.algrithm)
    logInfo(s"Saving to output path: $finalPath, algrithm: ${ctx.algrithm}")
    model.map(e => {
      val entity = e.asInstanceOf[EntityConversion[TaggedLogAPIEntity]].toEntity
      entity.setSerializeVerbose(false)
      TaggedLogAPIEntity.buildObjectMapper().writeValueAsString(entity)
    }).saveAsTextFile(finalPath)
  }
}

case class UserProfileEagleServiceSink(host:String = "localhost",port:Int = 9099, username:String = "admin", password:String = "secret", maxRetryTimes:Int = 100) extends UserProfileModelSink with Logging{
  @transient var client:IEagleServiceClient = null

  def persist(model: UserProfileModel): Unit = {
    if (model == null) {
      logDebug("model is null")
      return
    }
    if (client == null) client = new EagleServiceClientImpl(host, port, username, password)
    var triedTimes = 0
    var response: GenericServiceAPIResponseEntity[String] = null
    while (triedTimes < maxRetryTimes && (response == null || !response.isSuccess)) {
      triedTimes += 1
      try{
        response = client.create(util.Arrays.asList(model.asInstanceOf[EntityConversion[TaggedLogAPIEntity]].toEntity))
        if (response.isSuccess) {
          logInfo(s"Successfully persist 1 model for user: ${model.user} algorithm: ${model.algorithm}")
        } else {
          logError(s"Failed to persist 1 model for user: ${model.user} algorithm: ${model.algorithm}, tried $triedTimes times, exception: ${response.getException}")
        }
      } catch {
        case e: Throwable => {
          logError(s"Failed to persist for exception 1 model for user: ${model.user} algorithm: ${model.algorithm}, tried $triedTimes times, exception: ${e.getMessage}",e)
          response = null
        }
      }
    }
  }
}

case object UserProfileStdoutAsJsonSink extends UserProfileModelSink with Logging{
  val objMapper = new ObjectMapper
  override def persist(model: UserProfileModel): Unit = {
    val json = objMapper.writeValueAsString(model)
    println(json)
  }
}