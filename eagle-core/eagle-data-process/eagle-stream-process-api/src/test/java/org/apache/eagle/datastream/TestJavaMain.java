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
package org.apache.eagle.datastream;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Arrays;

public class TestJavaMain {
    public static class SerializableFunction1<T1,R> extends AbstractFunction1<T1, R> implements Serializable {
        @Override
        public Object apply(Object v1) {
            return null;
        }
    }

    //@Test
    public void testGeneral(){
        Config config = ConfigFactory.load();
        StormExecutionEnvironment env = ExecutionEnvironmentFactory.getStorm(config);
        env.newSource(new TestKeyValueSpout()).renameOutputFields(2).groupBy(Arrays.asList(0)).flatMap(new GroupedEchoExecutor()).withParallelism(2);
        env.execute();
    }

    //@Test
    public void testMap(){
        Config config = ConfigFactory.load();
        StormExecutionEnvironment env = ExecutionEnvironmentFactory.getStorm(config);
        SerializableFunction1 f1 = new SerializableFunction1<Object, Object>();
        env.newSource(new TestKeyValueSpout()).renameOutputFields(2).
                map1(f1);
        env.execute();
    }

    @Test
    public void test() {

    }
}
