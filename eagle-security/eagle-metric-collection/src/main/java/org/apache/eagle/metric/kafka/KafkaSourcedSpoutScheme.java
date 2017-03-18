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
package org.apache.eagle.metric.kafka;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.storm.kafka.SpoutKafkaMessageDeserializer;
import org.apache.storm.utils.Utils;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
            if(context.hasPath("eagleProps")) {
                prop.putAll(context.getObject("eagleProps"));
            }
			Constructor<?> constructor =  Class.forName(deserClsName).getConstructor(Properties.class);
			deserializer = (SpoutKafkaMessageDeserializer) constructor.newInstance(prop);
		}catch(Exception ex){
			throw new RuntimeException("Failed to create new instance for decoder class " + deserClsName, ex);
		}
	}

	@Override
	public List<Object> deserialize(ByteBuffer ser) {
		Object tmp = deserializer.deserialize(Utils.toByteArray(ser));
		Map<String, Object> map = (Map<String, Object>)tmp;
		if(tmp == null) return null;
		return Arrays.asList(map.get("user"), map.get("timestamp"));
	}

    /**
     * Default only f0, but it requires to be overrode if different
     * @return Fields
     */
	@Override
	public Fields getOutputFields() {
        return new Fields(NameConstants.FIELD_PREFIX+"0");
	}
}
