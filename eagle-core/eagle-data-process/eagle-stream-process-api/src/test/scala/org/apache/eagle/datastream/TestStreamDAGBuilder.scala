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
package org.apache.eagle.datastream

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class TestStreamDAGBuilder extends FlatSpec with Matchers{
//  "a single source DAG with groupBy" should "be traversed without groupBy node" in {
//    val config = ConfigFactory.load()
//    val env = ExecutionEnvironmentFactory.getStorm(config)
//    val tail = env.newSource(null).flatMap(EchoExecutor()).groupBy(0).flatMap(WordPrependExecutor("test"))
//    val dag = new StreamDAGBuilder(env.heads).build()
//    val iter = dag.iterator()
//    assert(iter.hasNext)
//    iter.next() match{
//      case StormSourceProducer(t) => assert(t == null)
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match{
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[EchoExecutor])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match{
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[WordPrependExecutor])
//      case _ => assert(false)
//    }
//  }
//
//  "a single source DAG with groupBy from spout" should "be traversed without groupBy node" in {
//    val config = ConfigFactory.load()
//    val env = ExecutionEnvironmentFactory.getStorm(config)
//    val tail = env.newSource(null).groupBy(0).flatMap(WordPrependExecutor("test"))
//    val dag = new StreamDAGBuilder(env.heads).build()
//    val iter = dag.iterator()
//    assert(iter.hasNext)
//    iter.next() match{
//      case StormSourceProducer(t) => assert(t == null)
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match{
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[WordPrependExecutor])
//      case _ => assert(false)
//    }
//  }
//
//  "a single source DAG with groupBy from spout and then split" should "be traversed without groupBy node" in {
//    val config = ConfigFactory.load()
//    val env = ExecutionEnvironmentFactory.getStorm(config)
//    val groupby = env.newSource(null).groupBy(0)
//    groupby.flatMap(WordPrependExecutor("test"))
//    groupby.flatMap(WordAppendExecutor("test"))
//    val dag = new StreamDAGBuilder(env.heads).build()
//    val iter = dag.iterator()
//    assert(iter.hasNext)
//    iter.next() match{
//      case StormSourceProducer(t) => assert(t == null)
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match{
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[WordPrependExecutor])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match{
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[WordAppendExecutor])
//      case _ => assert(false)
//    }
//  }
//
//  "a single source DAG without stream join" should "be traversed sequentially like specified" in{
//    val config = ConfigFactory.load()
//    val env = ExecutionEnvironmentFactory.getStorm(config)
//    val tail = env.newSource(null).flatMap(EchoExecutor()).flatMap(WordPrependExecutor("test"))
//    val dag = new StreamDAGBuilder(env.heads).build()
//    val iter = dag.iterator()
//    assert(iter.hasNext)
//    iter.next() match{
//      case StormSourceProducer(t) => assert(t == null)
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match{
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[EchoExecutor])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match{
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[WordPrependExecutor])
//      case _ => assert(false)
//    }
//  }
//
//  "a single source with split" should "has more than one tail producer" in {
//    val config = ConfigFactory.load()
//    val env = ExecutionEnvironmentFactory.getStorm(config)
//    val echo = env.newSource(null).flatMap(EchoExecutor())
//    val tail1 = echo.flatMap(WordPrependExecutor("test"))
//    val tail2 = echo.flatMap(WordAppendExecutor("test"))
//    val dag = new StreamDAGBuilder(env.heads).build()
//    val iter = dag.iterator()
//    assert(iter.hasNext)
//    iter.next() match {
//      case StormSourceProducer(t) => assert(t == null)
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[EchoExecutor])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[WordPrependExecutor])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[WordAppendExecutor])
//      case _ => assert(false)
//    }
//  }
//
//  "a single source with split and join" should "has join" in {
//    val config = ConfigFactory.load()
//    val env = ExecutionEnvironmentFactory.getStorm(config)
//    val echo = env.newSource(null).flatMap(EchoExecutor())
//    val tail1 = echo.flatMap(WordPrependExecutor("test"))
//    val tail2 = echo.flatMap(WordAppendExecutor("test")).filter(_=>true).streamUnion(List(tail1)).
//      flatMap(PatternAlertExecutor("test*"))
//    val dag = new StreamDAGBuilder(env.heads).build()
//    val iter = dag.iterator()
//    assert(iter.hasNext)
//    iter.next() match {
//      case StormSourceProducer(t) => assert(t == null)
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[EchoExecutor])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[WordPrependExecutor])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[WordAppendExecutor])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case FilterProducer(fn) =>
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case FlatMapProducer(worker) => assert(worker.asInstanceOf[PatternAlertExecutor].pattern.equals("test*"))
//      case _ => assert(false)
//    }
//    assert(!iter.hasNext)
//  }
//
//  "multiple sources with split and union" should "has union" in {
//    val config = ConfigFactory.load()
//    val env = ExecutionEnvironmentFactory.getStorm(config)
//    val source1 = env.newSource(TestSpout())
//    val source2 = env.newSource(TestSpout())
//    val source3 = env.newSource(TestSpout())
//
//    val tail1 = source1.flatMap(WordPrependExecutor("test"))
//    val tail2 = source2.filter(_=>true)
//    val tail3 = source3.flatMap(WordAppendExecutor("test")).streamUnion(List(tail1, tail2)).
//      flatMap(PatternAlertExecutor("abc*"))
//
//    val dag = new StreamDAGBuilder(env.heads).build()
//    val iter = dag.iterator()
//
//    assert(iter.hasNext)
//    iter.next() match {
//      case StormSourceProducer(t) => assert(t.isInstanceOf[TestSpout])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[WordPrependExecutor])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case StormSourceProducer(t) => assert(t.isInstanceOf[String])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case FilterProducer(fn) =>
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case SourceProducer(t) => assert(t.isInstanceOf[String])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case FlatMapProducer(worker) => assert(worker.isInstanceOf[WordAppendExecutor])
//      case _ => assert(false)
//    }
//    assert(iter.hasNext)
//    iter.next() match {
//      case FlatMapProducer(worker) => assert(worker.asInstanceOf[PatternAlertExecutor].pattern.equals("abc*"))
//      case _ => assert(false)
//    }
//    assert(!iter.hasNext)
//  }
}