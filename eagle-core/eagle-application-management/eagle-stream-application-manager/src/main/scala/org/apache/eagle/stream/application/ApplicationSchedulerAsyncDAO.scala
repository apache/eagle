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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.stream.application

import java.util
import java.util.concurrent.Callable

import akka.dispatch.Futures
import com.typesafe.config.Config
import org.apache.eagle.alert.entity.SiteApplicationServiceEntity
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity
import org.apache.eagle.policy.common.Constants
import org.apache.eagle.service.application.entity.{TopologyDescriptionEntity, TopologyExecutionEntity, TopologyOperationEntity}
import org.apache.eagle.service.client.EagleServiceConnector
import org.apache.eagle.service.client.impl.EagleServiceClientImpl
import org.apache.eagle.stream.application.model.TopologyDescriptionModel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions
import scala.concurrent.ExecutionContext


class ApplicationSchedulerAsyncDAO(config: Config, ex: ExecutionContext) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[ApplicationSchedulerAsyncDAO])
  private val connector: EagleServiceConnector = new EagleServiceConnector(config)

  def getEagleServiceClient(): EagleServiceClientImpl = {
    return new EagleServiceClientImpl(connector)
  }

  def readOperationsByStatus(status: String) = {
    Futures.future(new Callable[Option[util.List[TopologyOperationEntity]]]{
      override def call(): Option[util.List[TopologyOperationEntity]] = {
        val client = getEagleServiceClient()
        val query = "%s[@status=\"%s\"]{*}".format(Constants.TOPOLOGY_OPERATION_SERVICE_ENDPOINT_NAME, status)
        val response: GenericServiceAPIResponseEntity[TopologyOperationEntity] = client.search(query).pageSize(Int.MaxValue).send()
        if(client != null) client.close()
        if(response.getObj != null && response.getObj.size() != 0) Option(response.getObj) else None
      }
    }, ex)
  }

  def loadAllTopologyExecutionEntities() = {
    Futures.future(new Callable[Option[util.List[TopologyExecutionEntity]]]{
      override def call(): Option[util.List[TopologyExecutionEntity]] = {
        val client = getEagleServiceClient()
        val query = "%s[@status != \"%s\"]{*}".format(Constants.TOPOLOGY_EXECUTION_SERVICE_ENDPOINT_NAME, TopologyExecutionEntity.TOPOLOGY_STATUS.NEW)
        val response: GenericServiceAPIResponseEntity[TopologyExecutionEntity] = client.search(query).pageSize(Int.MaxValue).send()
        if(client != null) client.close()
        if(response.isSuccess && response.getObj != null && response.getObj.size() != 0) Option(response.getObj) else None
      }
    }, ex)
  }

  def loadTopologyExecutionByName(site: String, appName: String, topologyName: String) = {
    Futures.future(new Callable[Option[TopologyExecutionEntity]]{
      override def call(): Option[TopologyExecutionEntity] = {
        val client = getEagleServiceClient()
        val query = "%s[@site=\"%s\" AND @application=\"%s\" AND @topology=\"%s\"]{*}".format(Constants.TOPOLOGY_EXECUTION_SERVICE_ENDPOINT_NAME, site, appName, topologyName)
        LOG.info(s"query=$query")
        val response: GenericServiceAPIResponseEntity[TopologyExecutionEntity] = client.search(query).pageSize(Int.MaxValue).send()
        if(client != null) client.close()
        if(response.isSuccess && response.getObj != null && response.getObj.size() != 0) Option(response.getObj.get(0)) else None
      }
    }, ex)
  }

  def loadTopologyDescriptionByName(site: String, application: String, topologyName: String) = {
    Futures.future(new Callable[Option[TopologyDescriptionModel]]{
      override def call(): Option[TopologyDescriptionModel] = {
        val client = getEagleServiceClient()
        var query = "%s[@topology=\"%s\"]{*}".format(Constants.TOPOLOGY_DESCRIPTION_SERVICE_ENDPOINT_NAME, topologyName)
        val response: GenericServiceAPIResponseEntity[TopologyDescriptionEntity] = client.search(query).pageSize(Int.MaxValue).send()
        var topologyDescriptionModel: TopologyDescriptionModel = null

        if(response.isSuccess) {
          val topologyDescriptionEntity = response.getObj.get(0)
          topologyDescriptionModel = TopologyDescriptionEntity.toModel(topologyDescriptionEntity)
          query = "%s[@site=\"%s\" AND @application=\"%s\"]{*}".format(Constants.SITE_APPLICATION_SERVICE_ENDPOINT_NAME, site, application)
          val configResponse: GenericServiceAPIResponseEntity[SiteApplicationServiceEntity] = client.search(query).pageSize(Int.MaxValue).send()
          if (client != null) client.close()
          if (configResponse.getObj != null && configResponse.getObj.size() != 0) {
            val siteApplicationEntity = configResponse.getObj.get(0)
            topologyDescriptionModel.config = siteApplicationEntity.getConfig
          }
          if(configResponse.isSuccess && topologyDescriptionModel != null) Option(topologyDescriptionModel) else None
        } else {
          None
        }
      }
    }, ex)
  }

  def updateOperationStatus(operation: TopologyOperationEntity, status: String) = {
    Futures.future(new Callable[GenericServiceAPIResponseEntity[String]]{
      override def call(): GenericServiceAPIResponseEntity[String] = {
        if(LOG.isDebugEnabled()) LOG.debug(s"Updating status of command[$operation] as $status")
        val client = getEagleServiceClient()
        operation.setStatus(status)
        operation.setLastModifiedDate(System.currentTimeMillis())
        if(client != null) client.close()
        val response= client.update(java.util.Arrays.asList(operation), classOf[TopologyOperationEntity])
        if(response.isSuccess) {
          LOG.info(s"Updated operation status [$operation] as: $status")
        } else {
          LOG.error(s"Failed to update status as $status of command[$operation]")
          throw new RuntimeException(s"Failed to update command due to exception: ${response.getException}")
        }
        response
      }
    }, ex)
  }

  def updateTopologyExecutionStatus(topology: TopologyExecutionEntity, status: String) = {
    Futures.future(new Callable[GenericServiceAPIResponseEntity[String]]{
      override def call(): GenericServiceAPIResponseEntity[String] = {
        if(LOG.isDebugEnabled()) LOG.debug(s"Updating status of app[$topology] as $status")
        val client = getEagleServiceClient()
        topology.setStatus(status)
        topology.setLastModifiedDate(System.currentTimeMillis())
        if(client != null) client.close()
        val response= client.update(java.util.Arrays.asList(topology), classOf[TopologyExecutionEntity])
        if(response.isSuccess) {
          LOG.info(s"Updated status application[$topology] as: $status")
        } else {
          LOG.error(s"Failed to update status as $status of application[$topology]")
          throw new RuntimeException(s"Failed to update application due to exception: ${response.getException}")
        }
        response
      }
    }, ex)
  }

  def clearPendingOperations() = {
    Futures.future(new Callable[GenericServiceAPIResponseEntity[String]]{
      override def call(): GenericServiceAPIResponseEntity[String] = {
        LOG.info("start to clear operation")
        val query: String = "%s[@status=\"%s\"]{*}".format(Constants.TOPOLOGY_OPERATION_SERVICE_ENDPOINT_NAME, TopologyOperationEntity.OPERATION_STATUS.PENDING)
        val client = getEagleServiceClient()
        val response: GenericServiceAPIResponseEntity[TopologyOperationEntity] = client.search(query).pageSize(Int.MaxValue).send()
        var ret: GenericServiceAPIResponseEntity[String] = new GenericServiceAPIResponseEntity[String]()
        if (response.isSuccess && response.getObj.size != 0) {
          val pendingOperations: util.List[TopologyOperationEntity] = response.getObj
          val failedOperations: util.List[TopologyOperationEntity] = new util.ArrayList[TopologyOperationEntity]
          JavaConversions.collectionAsScalaIterable(pendingOperations) foreach { operation =>
            operation.setStatus(TopologyOperationEntity.OPERATION_STATUS.FAILED)
            failedOperations.add(operation)
          }
          ret = client.update(failedOperations, Constants.TOPOLOGY_OPERATION_SERVICE_ENDPOINT_NAME)
          if (client != null) client.close()
          if (ret.isSuccess) {
            LOG.info(s"Successfully clear ${failedOperations.size()} pending operations")
          } else {
            LOG.error(s"Failed to clear pending operations due to exception:" + ret.getException)
          }
        }
        ret
      }
    }, ex)
  }
}


