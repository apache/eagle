/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.stream.pipeline.compiler


import org.apache.eagle.datastream.core._
import org.apache.eagle.stream.pipeline.extension.ModuleManager._
import org.apache.eagle.stream.pipeline.parser._
import org.apache.eagle.stream.pipeline.utils.CompileException

trait PipelineCompiler {
  def compile(pipeline:Pipeline):StreamContext = {
    val context = new StreamContext(pipeline.config)
    val dataflow = pipeline.dataflow
    val dag = new StreamDAG(context.dag)
    dataflow.getProcessors.map(buildStreamProducer(dag,_)).foreach(producer =>{
      producer.initWith(dag.graph,pipeline.config)
      dag.addVertex(producer)
    })
    dataflow.getConnectors.foreach(connector =>{
      val from = dag.getNodeByName(connector.from).get
      val to = dag.getNodeByName(connector.to).get
      dag.addEdge(from,to,buildStreamConnector(from,to,dataflow,connector))
    })
    context
  }
  private def  buildStreamProducer(dag:StreamDAG,processor:Processor):StreamProducer[Any] = {
    if(findModuleType(processor.getType)){
      getModuleMapperByType(processor.getType).map(processor).nameAs(processor.getId).stream(processor.streamId)
    } else {
      throw new CompileException(s"Unknown processor type [${processor.getType}]")
    }
  }
  private def buildStreamConnector(from:StreamProducer[Any],to:StreamProducer[Any],dataflow:DataFlow,connector:Connector):StreamConnector[Any,Any]={
    var groupByIndexes:Seq[Int] = connector.groupByIndexes.orNull
    if(groupByIndexes!=null ){
      if(connector.groupByFields.isDefined) throw new CompileException(s"Both ${Connector.GROUP_BY_FIELD_FIELD} and ${Connector.GROUP_BY_INDEX_FIELD} is defined at same time")
    } else if(connector.groupByFields.isDefined){
      groupByIndexes = connector.groupByFields.get.map(dataflow.getProcessor(from.name).get.getSchema.get.indexOfAttribute)
    }
    if(groupByIndexes == null){
      ShuffleConnector(from,to)
    } else {
      GroupbyFieldsConnector(from,to,groupByIndexes)
    }
  }
}