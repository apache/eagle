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

import org.apache.eagle.security.userprofile.UserProfileUtils
import org.joda.time.{LocalDate, Period}

import scala.collection.mutable

/**
 * User Profile Daemon Schedule Policy
 * Configure scheduler by defining policy for managing status
 *
 * @since  9/5/15
 */
trait SchedulePolicy{
  /**
   * @param timestamp
   * @return
   */
  def validateStatus(timestamp:Long):Boolean

  /**
   *
   * @param timestamp
   * @return
   */
  def nextStatus(timestamp:Long):Long

  /**
   *
   * @param name
   * @return
   */
  def getStatus(name:String):Option[Long]

  /**
   *
   * @param name
   * @param status
   */
  def updateStatus(name:String,status:Long):Unit

  /**
   *
   * @param timestamp
   * @return
   */
  def formatStatus(timestamp:Long):Long
}

trait StateManager {
  def getState(name:String):Option[Long]
  def updateState(name:String,value:Long)
}

object MemoryStateManager extends StateManager {
  val map = new mutable.HashMap[String,Long]()
  override def getState(name: String): Option[Long] = this.synchronized { map.get(name) }
  override def updateState(name: String, value: Long): Unit = this.synchronized {
    map += (name -> value)
  }
}

object ZKStateManager extends StateManager{
  override def getState(name: String): Option[Long] = ???
  override def updateState(name: String, value: Long): Unit = ???
}

class DefaultSchedulePolicy(duration:Period,stateManager:StateManager = MemoryStateManager) extends SchedulePolicy{
  override def validateStatus(timestamp: Long): Boolean = formatStatus(timestamp) < formatStatus(System.currentTimeMillis())

//  override def nextStatus(timestamp: Long): Long = new LocalDate(timestamp).plus(duration).toDate.getTime
  override def nextStatus(timestamp: Long): Long = timestamp + Int.int2long(duration.toStandardSeconds.getSeconds) * 1000

  override def getStatus(name: String): Option[Long] = stateManager.getState(name)

  override def updateStatus(name: String, status: Long): Unit = stateManager.updateState(name,status)

  override def formatStatus(timestamp: Long): Long = UserProfileUtils.formatMillisecondsByPeriod(timestamp,duration)
}

class MonthlySchedulePolicy(stateManager:StateManager = MemoryStateManager) extends DefaultSchedulePolicy(duration = null,stateManager=stateManager){
  override def formatStatus(timestamp: Long): Long = new LocalDate(timestamp).withDayOfMonth(1).toDate.getTime
  override def nextStatus(timestamp: Long): Long = new LocalDate(timestamp).plusMonths(1).withDayOfMonth(1).toDate.getTime
}