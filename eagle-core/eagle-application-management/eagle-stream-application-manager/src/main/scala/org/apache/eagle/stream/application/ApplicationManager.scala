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
import java.util.concurrent._
import com.google.common.base.Preconditions
import org.apache.eagle.service.application.entity.TopologyExecutionStatus
import org.slf4j.{LoggerFactory, Logger}


object ApplicationManager {
  private val LOG: Logger = LoggerFactory.getLogger(ApplicationManager.getClass)
  private val threadPoolCoreSize: Int = 50
  private val threadPoolMaxSize: Int = 80
  private val threadPoolShrinkTime: Long = 60000L

  private val workerMap: util.Map[AnyRef, TaskExecutor] = new util.TreeMap[AnyRef, TaskExecutor]
  val executorService: ExecutorService = new ThreadPoolExecutor(threadPoolCoreSize, threadPoolMaxSize, threadPoolShrinkTime, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable])


  def getWorkerMap: util.Map[AnyRef, TaskExecutor] = {
    return workerMap
  }

  def submit(id: AnyRef, runnable: Runnable): TaskExecutor = {
    if (workerMap.containsKey(id)) {
      val executor: Thread = workerMap.get(id)
      if (!executor.isAlive || executor.getState.equals() ) {
        LOG.info("Replacing dead executor: {}", executor)
        workerMap.remove(id)
      }
      else {
        throw new IllegalArgumentException("Duplicated id '" + id + "'")
      }
    }
    val worker: TaskExecutor = new TaskExecutor(runnable)
    LOG.info("Registering new executor %s: %s".format(id, worker))
    workerMap.put(id, worker)
    worker.setName(id.toString)
    worker.setDaemon(true)
    worker.start
    return worker
  }

  def get(id: AnyRef): TaskExecutor = {
    Preconditions.checkArgument(workerMap.containsKey(id))
    return workerMap.get(id)
  }

  @throws(classOf[Exception])
  def stop(id: AnyRef): TaskExecutor = {
    val worker: TaskExecutor = get(id)
    worker.interrupt
    this.workerMap.remove(id)
    return worker
  }

  def getWorkerStatus(state: Thread.State): String = {
    if (whereIn(state, java.lang.Thread.State.RUNNABLE, java.lang.Thread.State.TIMED_WAITING, java.lang.Thread.State.WAITING)) {
      return TopologyExecutionStatus.STARTED
    }
    else if (whereIn(state, java.lang.Thread.State.NEW)) {
      return TopologyExecutionStatus.STARTING
    }
    else if (whereIn(state, java.lang.Thread.State.TERMINATED)) {
      return TopologyExecutionStatus.STOPPED
    }
    throw new IllegalStateException("Unknown state: " + state)
  }

  def getTopologyStatus(status: String): String = {
    if(!status.equalsIgnoreCase(TopologyExecutionStatus.STOPPED))
      TopologyExecutionStatus.STARTED
    else
      TopologyExecutionStatus.STOPPED
  }

  private def whereIn(state: Thread.State, inStates: Thread.State*): Boolean = {
    for (_state <- inStates) {
      if (_state eq state) {
        return true
      }
    }
    return false
  }

  def remove(id: AnyRef) {
    val executor: TaskExecutor = this.get(id)
    if (executor.isAlive) {
      throw new RuntimeException("Failed to remove alive executor '" + id + "'")
    }
    else {
      this.workerMap.remove(id)
    }
  }

}
