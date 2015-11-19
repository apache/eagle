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
package org.apache.eagle.dataproc.impl.storm.kafka;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import com.typesafe.config.Config;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Map;

/**
 * This scheme defines how a kafka message is deserialized and the output field name for storm stream
 * it includes the following:
 * 1. data source is kafka, so need kafka message deserializer class
 * 2. output field declaration
 */
public class KafkaSourcedSpoutScheme implements Scheme {
	protected SpoutKafkaMessageDeserializer deserializer;
	
	public KafkaSourcedSpoutScheme(String deserClsName, Config context){
		try{
			Properties prop = new Properties();
            if(context.getObject("eagleProps") != null) {
                prop.putAll(context.getObject("eagleProps"));
            }
			Constructor<?> constructor =  Class.forName(deserClsName).getConstructor(Properties.class);
			deserializer = (SpoutKafkaMessageDeserializer) constructor.newInstance(prop);
		}catch(Exception ex){
			throw new RuntimeException("Failed to create new instance for decoder class " + deserClsName, ex);
		}
	}
	
	@Override
	public List<Object> deserialize(byte[] ser) {
		Object tmp = deserializer.deserialize(ser);
		Map<String, Object> map = (Map<String, Object>)tmp;
		if(tmp == null)
			return null;
		// the following tasks are executed within the same process of kafka spout
		return Arrays.asList(tmp);
	}
	
	@Override
	public Fields getOutputFields() {
//		return new Fields(deserializer.getOutputFields());
		throw new UnsupportedOperationException("output fields should be declared in sub class of KafkaSourcedSpoutProvider");
	}
}
