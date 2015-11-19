/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.datastream

import java.util

import org.apache.eagle.alert.dedup.{AlertEmailDeduplicationExecutor, AlertEntityDeduplicationExecutor}
import org.apache.eagle.alert.notification.AlertNotificationExecutor
import org.apache.eagle.alert.persist.AlertPersistExecutor
import org.apache.eagle.executor.AlertExecutor
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 * Create alert executors and provide callback for programmer to link alert executor to immediate parent executors
 *
 * <br/><br/>
 * Explanations for programId, alertExecutorId and policy<br/><br/>
 * - programId - distributed or single-process program for example one storm topology<br/>
 * - alertExecutorId - one process/thread which executes multiple policies<br/>
 * - policy - some rules to be evaluated<br/>
 *
 * <br/>
 *
 * Normally the mapping is like following:
 * <pre>
 * programId (1:N) alertExecutorId
 * alertExecutorId (1:N) policy
 * </pre>
 */

object AlertExecutorConsumerUtils {
  private val LOG: Logger = LoggerFactory.getLogger(AlertExecutorConsumerUtils.getClass)

  def setupAlertConsumers(toBeAddedEdges: ListBuffer[StreamConnector], alertStreamProducers: List[StreamProducer]): Unit = {
    var alertExecutorIdList : java.util.List[String] = new util.ArrayList[String]()
    alertStreamProducers.map(x =>
      alertExecutorIdList.add(x.asInstanceOf[FlatMapProducer[AnyRef, AnyRef]].mapper.asInstanceOf[AlertExecutor].getAlertExecutorId));
    val alertDefDao = alertStreamProducers.head.asInstanceOf[FlatMapProducer[AnyRef, AnyRef]].mapper.asInstanceOf[AlertExecutor].getAlertDefinitionDao
    val entityDedupExecutor: AlertEntityDeduplicationExecutor = new AlertEntityDeduplicationExecutor(alertExecutorIdList, alertDefDao)
    val emailDedupExecutor: AlertEmailDeduplicationExecutor = new AlertEmailDeduplicationExecutor(alertExecutorIdList, alertDefDao)
    val notificationExecutor: AlertNotificationExecutor = new AlertNotificationExecutor(alertExecutorIdList, alertDefDao)
    val persistExecutor: AlertPersistExecutor = new AlertPersistExecutor

    val entityDedupStreamProducer = FlatMapProducer(UniqueId.incrementAndGetId(),entityDedupExecutor)
    val persistStreamProducer = FlatMapProducer(UniqueId.incrementAndGetId(),persistExecutor)
    val emailDedupStreamProducer = FlatMapProducer(UniqueId.incrementAndGetId(),emailDedupExecutor)
    val notificationStreamProducer = FlatMapProducer(UniqueId.incrementAndGetId(),notificationExecutor)
    toBeAddedEdges += StreamConnector(entityDedupStreamProducer, persistStreamProducer)
    toBeAddedEdges += StreamConnector(emailDedupStreamProducer, notificationStreamProducer)

    alertStreamProducers.foreach(sp => {
      toBeAddedEdges += StreamConnector(sp, entityDedupStreamProducer)
      toBeAddedEdges += StreamConnector(sp, emailDedupStreamProducer)
    })
  }
}
