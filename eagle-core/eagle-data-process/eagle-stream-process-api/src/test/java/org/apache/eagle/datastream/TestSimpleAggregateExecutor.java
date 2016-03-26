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
import junit.framework.Assert;
import org.apache.eagle.dataproc.impl.aggregate.SimpleAggregateExecutor;
import org.apache.eagle.dataproc.impl.aggregate.entity.AggregateEntity;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created on 1/20/16.
 */
public class TestSimpleAggregateExecutor {

    @Test
    public void test() throws Exception {
        SimpleAggregateExecutor sae = new SimpleAggregateExecutor(new String[]{"s1"},
                "define stream s1(eagleAlertContext object, timestamp long, metric string);" +
                        " @info(name='query')" +
                        " from s1 select * insert into tmp;"
                ,
                "siddhiCEPEngine",
                0,
                1);

        Config config = ConfigFactory.empty();
        sae.prepareConfig(config);
        sae.init();

        List<Object> tuple = new ArrayList<>(3);
        tuple.add(0, "groupbykey");
        tuple.add(1, "s1");
        SortedMap value = new TreeMap();
        value.put("timestamp", System.currentTimeMillis());
        value.put("metric", "name-of-the-metric");
        tuple.add(2, value);

        final AtomicInteger count = new AtomicInteger();
        sae.flatMap(tuple, new Collector<Tuple2<String, AggregateEntity>>(){
            @Override
            public void collect(Tuple2<String, AggregateEntity> stringAggregateEntityTuple2) {
                System.out.print(stringAggregateEntityTuple2._1());
                count.incrementAndGet();
            }
        });

        Thread.sleep(3000);
        Assert.assertEquals(1, count.get());
    }
}
