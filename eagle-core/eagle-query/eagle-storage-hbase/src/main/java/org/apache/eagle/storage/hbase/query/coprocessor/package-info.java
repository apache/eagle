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
/**
 *
 * <h1>Eagle Aggregation Coprocessor</h1>
 *
 * <h2>Deployment and Usage</h2>
 * <ol>
 * 	<li>
 *  Firstly deploy jar files to cluster on local file system or HDFS.<br/>
 * 	</li>
 * 	<li>
 * 	Secondly configure in <code>hbase-site.xml</code> as following:
 * 	<pre>&lt;property&gt;
 *   &lt;name>hbase.coprocessor.region.classes&lt;/name&gt;
 *   &lt;value>AggregateProtocolEndPoint&lt;/value&gt;
 * &lt;/property&gt;
 * 	</pre>
 * 	Or register on related hbase tables
 * 	<pre> hbase(main):005:0>  alter 't1', METHOD => 'table_att', 'coprocessor'=>'hdfs:///foo.jar|AggregateProtocolEndPoint|1001|'</pre>
 * 	</li>
 * 	<li>
 * <code>
 * AggregateClient client = new AggregateClientImpl();
 * client.aggregate
 * AggregateResult result = client.result
 * 
 * </code>
 * </li>
 * </ol>
 * 
 * <h2>Performance</h2>
 *
 * <b>NOTE:</b>
 * For single node of HBase, aggregation with coprocessor will be at least double faster than that on single client server,
 * and as HBase scale to more nodes, the aggregation performance will be linear scaled too, while aggregation on
 * client side will be more and more slow limit by computing and memory resource as well as network transmission on single
 * hot server machine.
 *
 * <br />
 * <br />
 * <b>A simple benchmark report for reference</b>
 * <br/>
 * <table border="1">
 *     <thead>
 *         <tr>
 *             <th>Region Servers</th> <th>Record Count</th>
 *             <th>Coprocessor</th><th>No-Coprocessor</th><th>Aggregation</th>
 *         </tr>
 *     </thead>
 *     <tbody>
 *         <tr>
 *             <td rowspan="10">1</td><td rowspan="10">1000,000</td>
 *             <td>10193 ms</td><td>21988 ms</td><td><@cluster,@datacenter>{count}</td>
 *         </tr>
 *         <tr>
 *             <td>10010 ms</td><td>22547 ms</td><td><@cluster,@datacenter>{sum(numTotalMaps)}</td>
 *         </tr>
 *         <tr>
 *             <td>10334 ms</td><td>23433 ms</td><td><@cluster,@datacenter>{avg(numTotalMaps)}</td>
 *         </tr>
 *         <tr>
 *             <td>10045 ms</td><td>22690 ms</td><td><@cluster,@datacenter>{max(numTotalMaps)}</td>
 *         </tr>
 *         <tr>
 *             <td>10190 ms</td><td>21902 ms</td><td><@cluster,@datacenter>{min(numTotalMaps)}</td>
 *         </tr>
 *     </tbody>
 * </table>
 * <h2>Reference</h2>
 * <a href="https://blogs.apache.org/hbase/entry/coprocessor_introduction">
 * 	Coprocessor Introduction 
 * </a>
 * (Trend Micro Hadoop Group: Mingjie Lai, Eugene Koontz, Andrew Purtell)
 * 
 * <h2>TO-DO</h2>
 * <ol>
 * <li>
 *   TODO: Pass writable self-described entity definition into HBase coprocessor instead of serviceName in String
 *
 *   Because using serviceName to get entity definition will reply on entity API code under eagle-app, so that
 *   when modifying or creating new entities, we have to update coprocessor jar in HBase side
 *   (hchen9@xyz.com)
 * </li>
 * <li>
 * 	 TODO: Using String.format instead substrings addition for long log to avoid recreating string objects
 * </li>
 * </ol>
 *
 * </table>
 * @see eagle.query.aggregate.coprocessor.AggregateClient
 * @see eagle.query.aggregate.coprocessor.AggregateResult
 * @see eagle.query.aggregate.coprocessor.AggregateProtocol
 * 
 * @since   : 11/10/14,2014
 */
package org.apache.eagle.storage.hbase.query.coprocessor;