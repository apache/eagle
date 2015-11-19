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

package org.apache.eagle.datastream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class JavaMapperStormExecutor extends BaseRichBolt{
    public static class e1 extends JavaMapperStormExecutor {
        public e1(JavaMapper mapper){
            super(1, mapper);
        }
    }
    public static class e2 extends JavaMapperStormExecutor {
        public e2(JavaMapper mapper){
            super(2, mapper);
        }
    }
    public static class e3 extends JavaMapperStormExecutor {
        public e3(JavaMapper mapper){
            super(3, mapper);
        }
    }
    public static class e4 extends JavaMapperStormExecutor {
        public e4(JavaMapper mapper){
            super(4, mapper);
        }
    }

    private JavaMapper mapper;
    private OutputCollector collector;
    private int numOutputFields;
    public JavaMapperStormExecutor(int numOutputFields, JavaMapper mapper){
        this.numOutputFields = numOutputFields;
        this.mapper = mapper;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        List<Object> ret = mapper.map(input.getValues());
        this.collector.emit(ret);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> fields = new ArrayList<String>();
        for(int i=0; i<numOutputFields; i++){
            fields.add(OutputFieldNameConst.FIELD_PREFIX() + i);
        }
        declarer.declare(new Fields(fields));
    }
}