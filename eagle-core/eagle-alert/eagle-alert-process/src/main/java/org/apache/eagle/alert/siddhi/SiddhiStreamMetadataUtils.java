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
package org.apache.eagle.alert.siddhi;

import java.util.Map;
import java.util.SortedMap;

import org.apache.eagle.alert.entity.AlertStreamSchemaEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * convert metadata entities for a stream to stream definition for siddhi cep engine
 * define stream HeapUsage (metric string, host string, value double, timestamp long)
 */
public class SiddhiStreamMetadataUtils {
	private final static Logger LOG = LoggerFactory.getLogger(SiddhiStreamMetadataUtils.class);
	
	public final static String EAGLE_ALERT_CONTEXT_FIELD = "eagleAlertContext";

	public static SortedMap<String, AlertStreamSchemaEntity> getAttrMap(String streamName) {
		SortedMap<String, AlertStreamSchemaEntity> map = StreamMetadataManager.getInstance().getMetadataEntityMapForStream(streamName);
		if(map == null || map.size() == 0){
			throw new IllegalStateException("alert stream schema should never be empty");
		}
		return map;
	}

	/**
     * @see org.wso2.siddhi.query.api.definition.Attribute.Type
     * make sure StreamMetadataManager.init is invoked before this method
	 * @param streamName
	 * @return
	 */
	public static String convertToStreamDef(String streamName){
		SortedMap<String, AlertStreamSchemaEntity> map = getAttrMap(streamName);
		StringBuilder sb = new StringBuilder();		
		sb.append(EAGLE_ALERT_CONTEXT_FIELD + " object,");
		for(Map.Entry<String, AlertStreamSchemaEntity> entry : map.entrySet()){
			String attrName = entry.getKey();
			sb.append(attrName);
			sb.append(" ");
			String attrType = entry.getValue().getAttrType();
			if(attrType.equalsIgnoreCase(AttributeType.STRING.name())){
				sb.append("string");
			}else if(attrType.equalsIgnoreCase(AttributeType.INTEGER.name())){
				sb.append("int");
			}else if(attrType.equalsIgnoreCase(AttributeType.LONG.name())){
				sb.append("long");
			}else if(attrType.equalsIgnoreCase(AttributeType.BOOL.name())){
				sb.append("bool");
			}else if(attrType.equalsIgnoreCase(AttributeType.FLOAT.name())){
                sb.append("float");
            }else if(attrType.equalsIgnoreCase(AttributeType.DOUBLE.name())){
                sb.append("double");
            }else{
				LOG.warn("AttrType is not recognized, ignore : " + attrType);
			}
			sb.append(",");
		}
		if(sb.length() > 0){
			sb.deleteCharAt(sb.length()-1);
		}
		
		String siddhiStreamDefFormat = "define stream " + streamName + "(" + "%s" + ");";
		return String.format(siddhiStreamDefFormat, sb.toString());
	}

	public static Object getAttrDefaultValue(String streamName, String attrName){
		SortedMap<String, AlertStreamSchemaEntity> map = getAttrMap(streamName);
		AlertStreamSchemaEntity entity = map.get(attrName);
		if (entity.getDefaultValue() != null) {
			return entity.getDefaultValue();
		}
		else {
			String attrType = entity.getAttrType();
			if (attrType.equalsIgnoreCase(AttributeType.STRING.name())) {
				return "NA";
			} else if (attrType.equalsIgnoreCase(AttributeType.INTEGER.name()) || attrType.equalsIgnoreCase(AttributeType.LONG.name())) {
				return -1;
			} else if (attrType.equalsIgnoreCase(AttributeType.BOOL.name())) {
				return true;
			} else {
				LOG.warn("AttrType is not recognized: " + attrType + ", treat it as string");
				return "N/A";
			}
		}
	}
}
