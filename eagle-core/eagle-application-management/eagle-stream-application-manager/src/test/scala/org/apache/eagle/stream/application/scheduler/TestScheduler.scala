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

package org.apache.eagle.stream.application.scheduler

import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{Ignore, BeforeAndAfterAll, MustMatchers, WordSpecLike}

@Ignore
class TestSchedulerSpec extends TestKit(ActorSystem("stream-app-scheduler"))
with WordSpecLike with MustMatchers with BeforeAndAfterAll {

  "A Scheduler actor" must {
    "Forward a message it receives" in {
      val coordinator = TestActorRef[StreamAppCoordinator]
      coordinator ! CommandLoaderEvent
      expectNoMsg()
    }
  }

  "A Integrated test" must {
    "run end-to-end" in {
      val coordinator = system.actorOf(Props[StreamAppCoordinator])
      coordinator ! CommandLoaderEvent
      expectNoMsg()
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    system.shutdown()
  }
}

@Ignore
object TestStreamAppScheduler extends App {
  val conf: String = """
                          akka.loglevel = "DEBUG"
                          akka.actor.debug {
                            receive = on
                            lifecycle = on
                          }
                     """
  new ApplicationScheduler().start(ConfigFactory.parseString(conf))
}