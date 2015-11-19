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
package org.apache.eagle.security.userprofile.job

import org.apache.commons.math3.linear.{Array2DRowRealMatrix, RealMatrix}
import org.apache.eagle.security.userprofile.UserProfileConstants
import org.apache.eagle.security.userprofile.model.AuditLogTransformer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.joda.time.Period

import scala.collection.mutable

/**
 * Audit Log UserProfileTrainingJob Implementation
 *
 * @param site site
 * @param input input path
 * @param master  spark master
 * @param appName spark application name
 * @param cmdTypes command features
 * @param period model aggregation granularity
 */
case class AuditLogTrainingSparkJob(
    site:String = null,
    var input:String=null,
    master:String="local[4]",
    appName:String=UserProfileConstants.DEFAULT_TRAINING_APP_NAME,
    cmdTypes:Seq[String] = UserProfileConstants.DEFAULT_CMD_TYPES,
    period:Period = Period.parse("PT1m")
  ) extends UserProfileTrainingJob with Logging {
  import org.apache.eagle.security.userprofile.job.AuditlogTrainingSparkJobFuncs._

  val conf = new SparkConf().setMaster(master).setAppName(appName)
  val sc = new SparkContext(conf)

  override def run(){
    logInfo(s"Starting $appName")
    buildDAG(sc)
    logInfo(s"Stopping $appName")
    sc.stop()
    logInfo(s"Finished $appName")
  }

  /**
   * Build Spark DAG
   *
   * @param sc
   */
  private def buildDAG(sc:SparkContext): Unit = {
    if (input == null) throw new IllegalArgumentException("input is null")

    val _site = site
    val _sc = sc
    val _cmdTypes = cmdTypes
    val _modelers = modelers
    val _modelSinks = modelSinks
    val _aggSinks = aggSinks

    val _period = period
    val tmp = _sc.textFile(input)
      .map(AuditLogTransformer(_period).transform)
      .filter(e => e.isDefined)
      .map(e => {((e.get.user,e.get.periodWindowSeconds,e.get.cmd),1.toDouble) }) // [(user,period,cmd),count]

    val _aggRDD = tmp
      .reduceByKey(_ + _)                                           // [(user,period,cmd),totalCount]
      .map(kv => {((kv._1._1, kv._1._2), (kv._1._3, kv._2))})       // [(user,period),(cmd,totalCount)]
      .groupByKey()
      .map(asUserCmdCountArray(_cmdTypes))                          // [(user,(period,array))]
      .sortBy(asUserPeriod, ascending = true)
      .groupByKey()
      .map(asMatrix)

    if (_modelers.size > 1 || _aggSinks.size > 1) _aggRDD.persist(StorageLevel.MEMORY_ONLY)

    _aggSinks.foreach(sink =>{
      sink.persist(_aggRDD,_cmdTypes,_site)
    })

    val modelRDDS = _modelers.map(
      modeler => {
        (_aggRDD.flatMap(v => {
          val _modeler = modeler
          _modeler.build(_site,v._1, v._2)
        }), modeler.context())
    })

    if(_modelSinks.size > 1) modelRDDS.foreach(_._1.cache())

    modelRDDS.foreach(kv => {
      _modelSinks.foreach(sink => {
        sink.persist(kv._1, kv._2)
      })
    })
  }

  def input(path:String):AuditLogTrainingSparkJob = {
    this.input = path
    this
  }
}

private object AuditlogTrainingSparkJobFuncs extends Logging{
  /**
   * From: [(user,period),(cmd,count)]
   *
   * To: [(user), (period,"getfileinfo", "open", "listStatus", "setTimes", "setPermission", "rename", "mkdirs", "create", "setReplication", "contentSummary", "delete", "setOwner", "fsck")]
   *
   * @param keyValue
   * @return
   */
  def asUserCmdCountArray(cmdTypes:Seq[String])(keyValue:(((String,Long),Iterable[(String,Double)])))={
    // make sure instance variables are always in local for closure
    val key = keyValue._1
    val it = keyValue._2
    var cmdCount = Map[String, Double]()
    it.foreach(k => {
      cmdCount += (k._1 -> k._2)
    })
    var cmdCounts = mutable.MutableList[Double]()
    cmdTypes.foreach(_cmdType => {
      cmdCounts += cmdCount.getOrElse[Double](_cmdType, 0.0)
    })
    val cmdCountsInDouble:Array[Double] = cmdCounts.toArray
    (key._1,(key._2,cmdCountsInDouble))
  }

  /**
   * @param pair
   * @return (user,matrix)
   */
  def asMatrix(pair:(String,Iterable[(Long,Array[Double])]))={
    val data = new mutable.MutableList[Array[Double]]
    pair._2.foreach(v => data += v._2)
    val matrix:RealMatrix = new Array2DRowRealMatrix(data.toArray)
    (pair._1,matrix)
  }

  def asUserPeriod(param:(String,(Long,Array[Double]))) ={
    (param._1, param._2._1)
  }
}