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

import org.apache.commons.configuration.{MapConfiguration, ConfigurationConverter}
import org.joda.time.Period

import scala.collection.JavaConversions

/**
 * Must be the same name with UserProfileTrainingCLI for class path loader
 *
 * @since  7/28/15
 */
class UserProfileTrainingCLI{}

object UserProfileTrainingCLI{
  val PARSER  = new scopt.OptionParser[UserProfileTrainingConfig]("[submit-command] --class eagle.security.userprofile.UserProfileTrainingMain [jar]") {
    head("User Profile Training Application","0.0.1")
    opt[String]('s',"site") required() action {(x,c) => c.copy(site = x)} text "Site"
    opt[String]('i',"input") required() action {(x,c) => c.copy(input = x)} text "Input audit log file path"
    opt[String]('o',"output") optional() action {(x,c) => c.copy(modelOutput = x)} text "Model output HDFS directory, final output path format is: ${output}_${algorithm}"
    opt[String]('m',"master") optional() action {(x,c) => c.copy(master = x)} text "Spark master url, default: local[10]"
    opt[String]('n',"app-name") optional() action {(x,c) => c.copy(appName = x)} text "Application name, default: UserProfile"
    opt[Seq[String]]('c',"cmds") optional() action {(x,c) => c.copy(cmdTypes = x)} text s"Command types, default: [${UserProfileConstants.DEFAULT_CMD_TYPES.mkString(",")}]"
    opt[String]('h',"service-host") optional() action {(x,c) => c.copy(serviceHost = x)} text "Eagle service host, default: localhost"
    opt[Int]('p',"service-port") optional()  action {(x,c) => c.copy(servicePort = x)} text "Eagle service port, default: 9099"
    opt[String]('u',"service-username") optional() action {(x,c) => c.copy(username = x)} text "Eagle service authentication username, default: admin"
    opt[String]('w',"service-password") optional() action {(x,c) => c.copy(password = x)} text "Eagle service authentication password, default: secure"
    opt[Map[String,String]]('k',"kafka-props") optional()  action { (x,c) => {c.copy(kafkaProps = ConfigurationConverter.getProperties(new MapConfiguration(JavaConversions.asJavaMap(x))))}} text "Kafka properties, for example: topic=sometopic,metadata.brokers.list=localhost:9200"
    opt[String]('r',"period") optional() action {(x,c) => c.copy(period = Period.parse(x))} text "Period window (https://en.wikipedia.org/wiki/ISO_8601#Durations), default: PT1M" // changing it to 1M interval
  }

  def main(args:Array[String]): Unit = {
    PARSER.parse(args,UserProfileTrainingConfig()) match {
      case Some(config) =>
        UserProfileTrainingApp(config)
        sys.exit(0)
      case None =>
        System.err.println("\nMore details: https://spark.apache.org/docs/latest/submitting-applications.html")
        sys.exit(1)
    }
  }
}