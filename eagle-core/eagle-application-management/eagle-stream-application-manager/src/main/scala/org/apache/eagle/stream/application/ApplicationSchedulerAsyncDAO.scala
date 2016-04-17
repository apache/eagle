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
import org.apache.eagle.service.application.entity.{TopologyDescriptionEntity, TopologyExecutionEntity, TopologyExecutionStatus, TopologyOperationEntity}
import org.apache.eagle.service.client.EagleServiceConnector
import org.apache.eagle.service.client.impl.EagleServiceClientImpl
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
    Futures.future(new Callable[util.List[TopologyOperationEntity]]{
      override def call(): util.List[TopologyOperationEntity] = {
        val client = getEagleServiceClient()
        val query = "%s[@status=\"%s\"]{*}".format(Constants.TOPOLOGY_OPERATION_SERVICE_ENDPOINT_NAME, status)
        val response: GenericServiceAPIResponseEntity[TopologyOperationEntity] = client.search(query).pageSize(Int.MaxValue).send()
        if(client != null) client.close()
        if(!response.isSuccess || response.getObj == null)
          throw new Exception(s"Fail to load operations with status $status")
        response.getObj
      }
    }, ex)
  }

  def loadAllTopologyExecutionEntities() = {
    Futures.future(new Callable[util.List[TopologyExecutionEntity]]{
      override def call(): util.List[TopologyExecutionEntity] = {
        val client = getEagleServiceClient()
        val query = "%s[@status != \"%s\"]{*}".format(Constants.TOPOLOGY_EXECUTION_SERVICE_ENDPOINT_NAME, TopologyExecutionStatus.NEW)
        val response: GenericServiceAPIResponseEntity[TopologyExecutionEntity] = client.search(query).pageSize(Int.MaxValue).send()
        if(client != null) client.close()
        if(!response.isSuccess || response.getObj == null) throw new Exception(response.getException)
        response.getObj
      }
    }, ex)
  }

  def loadTopologyExecutionByName(site: String, appName: String, topologyName: String) = {
    Futures.future(new Callable[TopologyExecutionEntity]{
      override def call(): TopologyExecutionEntity = {
        val client = getEagleServiceClient()
        val query = "%s[@site=\"%s\" AND @application=\"%s\" AND @topology=\"%s\"]{*}".format(Constants.TOPOLOGY_EXECUTION_SERVICE_ENDPOINT_NAME, site, appName, topologyName)
        LOG.info(s"query=$query")
        val response: GenericServiceAPIResponseEntity[TopologyExecutionEntity] = client.search(query).pageSize(Int.MaxValue).send()
        if(client != null) client.close()
        if(!response.isSuccess || response.getObj == null)
          throw new Exception(s"Fail to load topologyExecutionEntity with application=$appName topology=$topologyName due to Exception: ${response.getException}")
        if(response.getObj.size() == 0 || response.getObj.size() > 1) {
          throw new Exception(s"Get 0 or more than 1 topologyExecutionEntity with application=$appName topology=$topologyName")
        }
        response.getObj.get(0)
      }
    }, ex)
  }

  def loadTopologyDescriptionByName(site: String, application: String, topologyName: String) = {
    Futures.future(new Callable[TopologyDescriptionEntity]{
      override def call(): TopologyDescriptionEntity = {
        val client = getEagleServiceClient()
        var query = "%s[@topology=\"%s\"]{*}".format(Constants.TOPOLOGY_DESCRIPTION_SERVICE_ENDPOINT_NAME, topologyName)
        val response: GenericServiceAPIResponseEntity[TopologyDescriptionEntity] = client.search(query).pageSize(Int.MaxValue).send()
        if(!response.isSuccess || response.getObj == null || response.getObj.size() == 0)
          throw new Exception(s"Fail to load TopologyDescriptionEntity with site=$site application=$application topology=$topologyName due to Exception: ${response.getException}")
        val topologyDescriptionEntity = response.getObj.get(0)

        query = "%s[@site=\"%s\" AND @application=\"%s\"]{*}".format(Constants.SITE_APPLICATION_SERVICE_ENDPOINT_NAME, site, application)
        val configResponse: GenericServiceAPIResponseEntity[SiteApplicationServiceEntity] = client.search(query).pageSize(Int.MaxValue).send()
        if (client != null) client.close()
        if(!configResponse.isSuccess || configResponse.getObj == null || configResponse.getObj.size() == 0)
          throw new Exception(s"Fail to load topology configuration with query=$query due to Exception: ${configResponse.getException}")
        val siteApplicationEntity = configResponse.getObj.get(0)
        topologyDescriptionEntity.setContext(siteApplicationEntity.getConfig)
        topologyDescriptionEntity
      }
    }, ex)
  }

  def updateOperationStatus(operation: TopologyOperationEntity) = {
    Futures.future(new Callable[GenericServiceAPIResponseEntity[String]]{
      override def call(): GenericServiceAPIResponseEntity[String] = {
        if(LOG.isDebugEnabled()) LOG.debug(s"Updating status of command[$operation] as ${operation.getStatus}")
        val client = getEagleServiceClient()
        operation.setLastModifiedDate(System.currentTimeMillis())
        val response= client.update(java.util.Arrays.asList(operation), classOf[TopologyOperationEntity])
        if(client != null) client.close()
        if(response.isSuccess) {
          LOG.info(s"Updated operation status [$operation] as: ${operation.getStatus}")
        } else {
          LOG.error(s"Failed to update status as ${operation.getStatus} of command[$operation]")
          throw new RuntimeException(s"Failed to update command due to exception: ${response.getException}")
        }
        response
      }
    }, ex)
  }

  def updateTopologyExecutionStatus(topology: TopologyExecutionEntity) = {
    Futures.future(new Callable[GenericServiceAPIResponseEntity[String]]{
      override def call(): GenericServiceAPIResponseEntity[String] = {
        if(LOG.isDebugEnabled()) LOG.debug(s"Updating status of app[$topology] as ${topology.getStatus}")
        val client = getEagleServiceClient()
        topology.setLastModifiedDate(System.currentTimeMillis())
        if(client != null) client.close()
        val response= client.update(java.util.Arrays.asList(topology), classOf[TopologyExecutionEntity])
        if(response.isSuccess) {
          LOG.info(s"Updated status application[$topology] as: ${topology.getStatus}")
        } else {
          LOG.error(s"Failed to update status as ${topology.getStatus} of application[$topology] due to ${response.getException}")
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


