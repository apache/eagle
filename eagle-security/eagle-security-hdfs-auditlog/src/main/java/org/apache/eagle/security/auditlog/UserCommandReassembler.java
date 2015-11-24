/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.eagle.security.auditlog;

import com.typesafe.config.Config;
import org.apache.eagle.alert.siddhi.AttributeType;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.datastream.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.*;

/**
 * Created by yonzhang on 11/20/15.
 */
public class UserCommandReassembler extends JavaStormStreamExecutor2<String, Map> {
    private static final Logger LOG = LoggerFactory.getLogger(UserCommandReassembler.class);
    private Config config;
    private InputHandler inputHandler;
    /**
     * event schema is attribute name/type pairs
     */
    private SortedMap<String, String> eventSchema = new TreeMap<String, String>(){{
        put("timestamp", AttributeType.LONG.name());
        put("src", AttributeType.STRING.name());
        put("dst", AttributeType.STRING.name());
        put("host", AttributeType.STRING.name());
        put("allowed", AttributeType.STRING.name());
        put("user", AttributeType.STRING.name());
        put("cmd", AttributeType.STRING.name());
    }};
    // predefined attributes :  timestamp, src, dst, host, allowed, user, cmd
    private String streamDef = "define stream eventStream (context object, timeStamp long, src string, dst string, host string, allowed string, user string, cmd string);";

    @Override
    public void prepareConfig(Config config) {
        this.config = config;
    }

    @Override
    public void init() {
        SiddhiManager siddhiManager = new SiddhiManager();
        String query = "@info(name = 'query1') from " +
                "every a = eventStream[cmd=='getfileinfo'] " +
                "-> b = eventStream[cmd=='append' and user==a.user and src==a.src] " +
                "-> c = eventStream[cmd=='getfileinfo'and user==a.user and src==a.src] " +
                "select c.context, a.user as user, b.cmd as cmd, a.src as src " +
                "insert into outputStreams";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streamDef + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, final Event[] inEvents, Event[] removeEvents) {
                Collector<Tuple2<String, Map>> collector = (Collector<Tuple2<String, Map>>)inEvents[0].getData(0);
                String user = (String)inEvents[0].getData(1);
                // recreate map
                Map map = new HashMap(){{
                    put("user", inEvents[0].getData(1));
                    put("cmd", inEvents[0].getData(2));
                    put("src", inEvents[0].getData(3));
                }};

                collector.collect(new Tuple2<String, Map>(user, map));
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        inputHandler = executionPlanRuntime.getInputHandler("eventStream");
        executionPlanRuntime.start();
    }

    @Override
    public void flatMap(List<Object> input, Collector<Tuple2<String, Map>> collector) {
        Map map = (Map)input.get(1);
        try {
            inputHandler.send(new Object[]{collector, map.get("timestamp"), map.get("src"), map.get("dst"), map.get("host"), map.get("allowed"), map.get("user"), map.get("cmd")});
        }catch(Exception ex){
            LOG.error("fail sending event to Siddhi pattern engine", ex);
            throw new IllegalStateException(ex);
        }
    }
}
