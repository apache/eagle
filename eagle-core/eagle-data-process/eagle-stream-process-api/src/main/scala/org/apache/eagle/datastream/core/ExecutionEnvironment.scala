/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.datastream.core

import com.typesafe.config.Config

trait StreamContextAdapter{
  def submit(context:StreamContext):Unit = {
    execute(context.build)
  }
  def execute(dag: StreamDAG)
}

/**
 * TODO: Decouple execution environment with stream context
 *
 * @since 0.3.0
 */
abstract class ExecutionEnvironment(private val conf:Config)
  extends StreamContext(conf) with StreamContextAdapter     // Continue to support old API
  with StreamSourceBuilder
{
  /**
   * Start to execute
   */
  def execute():Unit = {
    submit(this)
  }
}