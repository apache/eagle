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
package org.apache.eagle.log.entity;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.meta.EntitySerDeser;
import org.apache.eagle.log.entity.meta.Qualifier;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EntityQualifierUtils {
	private final static Logger LOG = LoggerFactory.getLogger(EntityQualifierUtils.class);

	public static Map<String,Object> keyValuesToMap(List<KeyValue> row,EntityDefinition ed){
		Map<String,Object> result = new HashMap<String,Object>();
		for(KeyValue kv:row){
			String qualifierName = new String(kv.getQualifier());
			if(!ed.isTag(qualifierName)){
				Qualifier qualifier = ed.getDisplayNameMap().get(qualifierName);
				if(qualifier == null){
					qualifier = ed.getQualifierNameMap().get(qualifierName);
				}
				qualifierName = qualifier.getDisplayName();
				Object value = qualifier.getSerDeser().deserialize(kv.getValue());
				result.put(qualifierName,value);
			}else{
				result.put(qualifierName,new String(kv.getValue()));
			}
		}
		return result;
	}

	public static Map<String,Double> keyValuesToDoubleMap(List<KeyValue> row,EntityDefinition ed){
		Map<String,Double> result = new HashMap<String,Double>();
		for(KeyValue kv:row){
			String qualifierName = new String(kv.getQualifier());
			if(!ed.isTag(qualifierName)){
				Qualifier qualifier = ed.getDisplayNameMap().get(qualifierName);
				if(qualifier == null){
					qualifier = ed.getQualifierNameMap().get(qualifierName);
				}
				qualifierName = qualifier.getDisplayName();
				Object value = qualifier.getSerDeser().deserialize(kv.getValue());
				result.put(qualifierName,convertObjToDouble(value));
			}else{
				result.put(qualifierName,Double.NaN);
			}
		}
		return result;
	}

	/**
	 * Map[Display Name,Double Value]
	 *
	 * @param map
	 * @param ed
	 * @return
	 */
	public static Map<String,Double> bytesMapToDoubleMap(Map<String,byte[]> map,EntityDefinition ed){
		Map<String,Double> result = new HashMap<String,Double>();
		for(Map.Entry<String,byte[]> entry:map.entrySet()){
			String qualifierName = entry.getKey();
			Qualifier qualifier = ed.getDisplayNameMap().get(qualifierName);
			if(qualifier == null) qualifier = ed.getQualifierNameMap().get(qualifierName);
			if(qualifier!=null && entry.getValue()!=null) {
				qualifierName = qualifier.getDisplayName();
				Object value = qualifier.getSerDeser().deserialize(entry.getValue());
				result.put(qualifierName, convertObjToDouble(value));
			}else{
				result.put(qualifierName,null);
			}
		}
		return result;
	}

	public static byte[] toBytes(EntityDefinition ed, String qualifierName, String qualifierValueInStr){
		// Get field type from entity class
		// and skip for not-found fields query expression
		Object typedValue = null;
		EntitySerDeser serDeser = null;
		if(ed.isTag(qualifierName)){
			typedValue = qualifierValueInStr;
			serDeser = EntityDefinitionManager.getSerDeser(String.class);
		}else{
			try{
				Field field = ed.getEntityClass().getDeclaredField(qualifierName);
				Class<?> fieldType = field.getType();
				serDeser =  EntityDefinitionManager.getSerDeser(fieldType);
				if(serDeser == null){
					throw new IllegalArgumentException("Can't find EntitySerDeser for field: "+ qualifierName +"'s type: "+fieldType
							+", so the field is not supported to be filtered yet");
				}
				typedValue = convertStringToObject(qualifierValueInStr, fieldType);
			} catch (NoSuchFieldException ex) {
				// Handle the field not found exception in caller
				LOG.error("Field " + qualifierName + " not found in " + ed.getEntityClass());
				throw new IllegalArgumentException("Field "+qualifierName+" not found in "+ed.getEntityClass(),ex);
			}
		}
		return serDeser.serialize(typedValue);
	}

	public static Class<?> getType(EntityDefinition ed, String qualifierName) {
		Field field;
		try {
			field = ed.getEntityClass().getDeclaredField(qualifierName);
		} catch (NoSuchFieldException e) {
			if(LOG.isDebugEnabled()) LOG.debug("Field "+qualifierName+" not found in "+ed.getEntityClass());
			return null;
		}
		return field.getType();
	}

	/**
	 * Not support negative numeric value:
	 * - http://en.wikipedia.org/wiki/Double-precision_floating-point_format
	 *
	 * @param value
	 * @param type
	 * @return
	 */
	public static Object convertStringToObject(String value, Class<?> type){
		Object obj = null;
		try{
			if(String.class.equals(type)){
				obj =  value;
			}if(Long.class.equals(type) || long.class.equals(type)){
				obj = Long.parseLong(value);
				// if((Long) obj < 0) throw new IllegalArgumentException("Don't support negative Long yet: "+obj);
			}else if(Integer.class.equals(type) || int.class.equals(type)){
				obj = Integer.parseInt(value);
				// if((Integer) obj < 0) throw new IllegalArgumentException("Don't support negative Integer yet: "+obj);
			}else if(Double.class.equals(type) || double.class.equals(type)){
				obj = Double.parseDouble(value);
				// if((Double) obj < 0) throw new IllegalArgumentException("Don't support negative Double yet: "+obj);
			}else if(Float.class.equals(type) || float.class.equals(type)){
				obj = Float.parseFloat(value);
				// if((Double) obj < 0) throw new IllegalArgumentException("Don't support negative Float yet: "+obj);
			}
			if(obj != null) return obj;
		}catch (NumberFormatException ex){
			throw new IllegalArgumentException("Fail to convert string: "+value +" into type of "+type,ex);
		}

		throw new IllegalArgumentException("Fail to convert string: "+value +" into type of "+type+", illegal type: "+type);
	}

	/**
	 *
	 * @param obj
	 * @return double value, otherwise Double.NaN
	 */
	public static double convertObjToDouble(Object obj){
		if(Long.class.equals(obj.getClass()) || long.class.equals(obj.getClass())){
			Long _value = (Long) obj;
			return _value.doubleValue();
		}else if(Integer.class.equals(obj.getClass()) || int.class.equals(obj.getClass())){
			Integer _value = (Integer) obj;
			return _value.doubleValue();
		}else if(Double.class.equals(obj.getClass()) || double.class.equals(obj.getClass())) {
			return (Double) obj;
		}else if(Float.class.equals(obj.getClass()) || float.class.equals(obj.getClass())) {
			Float _value = (Float) obj;
			return _value.doubleValue();
		}else if(Short.class.equals(obj.getClass()) || short.class.equals(obj.getClass())) {
			Float _value = (Float) obj;
			return _value.doubleValue();
		}else if(Byte.class.equals(obj.getClass()) || byte.class.equals(obj.getClass())) {
			Byte _value = (Byte) obj;
			return _value.doubleValue();
		}
		LOG.warn("Failed to convert object " + obj.toString() + " in type of " + obj.getClass() + " to double");
		return Double.NaN;
	}

	/**
	 * Parse List String as Set without duplicate items
	 *
	 * <br></br>
	 * Support:
	 * <ul>
	 * <li>normal string: ("a","b") => ["a","b"] </li>
	 * <li>number: (1.5,"b") => [1.5,"b"] </li>
	 * <li>inner string comma: ("va,lue","value",",") => ["va,lue","value",","]</li>
	 * <li>inner escaped chars: ("va\"lue","value") => ["va\"lue","value"]</li>
	 * <li>some bad formats list: ("va"lue","value") => ["va\"lue","value"]</li>
	 * </ul>
	 *
	 * <b>Warning:</b> it will not throw exception if the format is not strictly valid
	 *
	 * @param listValue in format (item1,item2,...)
	 * @return
	 */
	public static List<String> parseList(String listValue){
		Matcher matcher = SET_PATTERN.matcher(listValue);
		if(matcher.find()){
			String content = matcher.group(1);
			List<String> result = new ArrayList<String>();
			StringBuilder str = null;
			STATE state = null;
			char last = 0;
			for(char c: content.toCharArray()){
				if(str == null) str = new StringBuilder();
				if(c == DOUBLE_QUOTE && last != SLASH){
					// Open or Close String
					if(state == STATE.STRING)
						state = null;
					else state = STATE.STRING;
				}else if(c == COMMA && state != STATE.STRING){
					result.add(unescape(str.toString()));
					str = null;
					last = c;
					continue;
				}
				last = c;
				str.append(c);
			}
			if(str!=null) result.add(unescape(str.toString()));
			return result;
		}else{
			LOG.error("Invalid list value: " + listValue);
			throw new IllegalArgumentException("Invalid format of list value: "+listValue+", must be in format: (item1,item2,...)");
		}
	}

	private static String unescape(String str){
		int start=0,end = str.length();
		if(str.startsWith("\"")) start = start +1;
		if(str.endsWith("\"")) end = end -1;
		str = str.substring(start,end);
		return StringEscapeUtils.unescapeJava(str);
	}

	private final static Pattern SET_PATTERN = Pattern.compile("^\\((.*)\\)$");
	private final static char COMMA = ',';
	private final static char DOUBLE_QUOTE = '"';
	private final static char SLASH = '\\';
	private static enum STATE{ STRING }



//  TODO: NOT FINISHED
//  private final static Map<String,String> ESCAPE_REGEXP=new HashMap<String,String>(){{
//			this.put("\\.","\\\\.");
//	}};
//
//	public static String escapeRegExp(String value) {
//		String _value = value;
//		for(Map.Entry<String,String> entry:ESCAPE_REGEXP.entrySet()){
//			_value = _value.replace(entry.getKey(),entry.getValue());
//		}
//		return _value;
//	}
}