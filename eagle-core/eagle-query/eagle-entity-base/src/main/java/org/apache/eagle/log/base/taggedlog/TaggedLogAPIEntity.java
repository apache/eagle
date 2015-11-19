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
package org.apache.eagle.log.base.taggedlog;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.annotate.JsonFilter;
import org.codehaus.jackson.map.ser.BeanPropertyWriter;
import org.codehaus.jackson.map.ser.FilterProvider;
import org.codehaus.jackson.map.ser.impl.SimpleBeanPropertyFilter;
import org.codehaus.jackson.map.ser.impl.SimpleFilterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.common.DateTimeUtil;

/**
 * rowkey: prefix + timestamp + tagNameValues
 * as of now, all tags will be persisted as a column in hbase table
 * tag name is column qualifier name
 * tag value is column value
 */
@JsonFilter(TaggedLogAPIEntity.PropertyBeanFilterName)
public class TaggedLogAPIEntity implements PropertyChangeListener, Serializable{
	private static final Logger LOG = LoggerFactory.getLogger(TaggedLogAPIEntity.class);
	private String prefix;
	private long timestamp;
	private Map<String, String> tags;

	public void setExp(Map<String, Object> exp) {
		this.exp = exp;
	}

	public Map<String,Object> getExp() {
		return this.exp;
	}

	/**
	 * Extra dynamic attributes. 
	 * TODO: can we move exp, _serializeAlias, _serializeVerbose to a wrapper class? 
	 */
	private Map<String,Object> exp;

	private String encodedRowkey;
	// track what qualifiers are changed
	private Set<String> _modifiedProperties = new HashSet<String>();
	protected PropertyChangeSupport _pcs = new PropertyChangeSupport(this);


	public Map<String, String> getSerializeAlias() {
		return _serializeAlias;
	}

	public void setSerializeAlias(Map<String, String> _serializeAlias) {
		this._serializeAlias = _serializeAlias;
	}

	private Map<String,String> _serializeAlias = null;

	public boolean isSerializeVerbose() {
		return _serializeVerbose;
	}

	public void setSerializeVerbose(boolean _serializeVerbose) {
		this._serializeVerbose = _serializeVerbose;
	}

	private boolean _serializeVerbose = true;

	public TaggedLogAPIEntity(){
		_pcs.addPropertyChangeListener(this);
	}
	
	public String getPrefix() {
		return prefix;
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public Map<String, String> getTags() {
		return tags;
	}
	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}
	public String getEncodedRowkey() {
		return encodedRowkey;
	}
	public void setEncodedRowkey(String encodedRowkey) {
		this.encodedRowkey = encodedRowkey;
	}
	
	protected void valueChanged(String fieldModified){
		_pcs.firePropertyChange(fieldModified, null, null);
	}
	
	public void propertyChange(PropertyChangeEvent evt) {
		_modifiedProperties.add(evt.getPropertyName());
	}
	
	public Set<String> modifiedQualifiers(){
		return this._modifiedProperties;
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("prefix:");
		sb.append(prefix);
		sb.append(", timestamp:");
		sb.append(timestamp);
		sb.append(", humanReadableDate:");
		sb.append(DateTimeUtil.millisecondsToHumanDateWithMilliseconds(timestamp));
		sb.append(", tags: ");
		if(tags != null){
			for(Map.Entry<String, String> entry : tags.entrySet()){
				sb.append(entry.toString());
				sb.append(",");
			}
		}
		sb.append(", encodedRowkey:");
		sb.append(encodedRowkey);
		return sb.toString();
	}

	private static Set<String> getPropertyNames(){
		if(_propertyNames == null) {
			Field[] fields = TaggedLogAPIEntity.class.getDeclaredFields();
			Set<String> fieldName = new HashSet<String>();
			for (Field f : fields) fieldName.add(f.getName());
			_propertyNames = fieldName;
		}
		return _propertyNames;
	}

	private static class BeanPropertyFilter extends SimpleBeanPropertyFilter {
		private final static String prefix = "prefix";
		private final static String encodedRowkey = "encodedRowkey";
		private final static String exp = "exp";
		private final static String timestamp = "timestamp";
		@SuppressWarnings("serial")
		private final static Set<String> verboseFields = new HashSet<String>(){{
			add(prefix);
			add(encodedRowkey);
		}};

		@Override
		public void serializeAsField(Object bean, JsonGenerator jgen, SerializerProvider provider, BeanPropertyWriter writer) throws Exception {
			if(bean instanceof TaggedLogAPIEntity){
				TaggedLogAPIEntity entity = (TaggedLogAPIEntity) bean;
				Set<String> modified = entity.modifiedQualifiers();
				Set<String> basePropertyNames = getPropertyNames();
				String writerName = writer.getName();
				if(modified.contains(writerName) || basePropertyNames.contains(writerName)){
					if((!entity.isSerializeVerbose() && verboseFields.contains(writerName))||							// skip verbose fields
							(timestamp.equals(writerName) && !EntityDefinitionManager.isTimeSeries(entity.getClass()))	// skip timestamp for non-timeseries entity
					){
						// log skip
						if(LOG.isDebugEnabled()) LOG.debug("skip field");
					}else{
						// if serializeAlias is not null and exp is not null
						if (exp.equals(writerName) && entity.getSerializeAlias()!=null && entity.getExp()!=null) {
							Map<String, Object> _exp = new HashMap<String, Object>();
							for (Map.Entry<String, Object> entry : entity.getExp().entrySet()) {
								String alias = entity.getSerializeAlias().get(entry.getKey());
								if (alias != null) {
									_exp.put(alias, entry.getValue());
								} else {
									_exp.put(entry.getKey(), entry.getValue());
								}
							}
							entity.setExp(_exp);
						}
						// write included field into serialized json output
						writer.serializeAsField(bean, jgen, provider);
					}
				}
			}else{
				writer.serializeAsField(bean, jgen, provider);
			}
		}
	}

	public static FilterProvider getFilterProvider(){
		if(_filterProvider == null){
			SimpleFilterProvider _provider = new SimpleFilterProvider();
			_provider.addFilter(PropertyBeanFilterName,new BeanPropertyFilter());
			_filterProvider = _provider;
		}
		return _filterProvider;
	}

	//////////////////////////////////////
	// Static fields
	//////////////////////////////////////
	private static Set<String> _propertyNames = null;
	private static FilterProvider _filterProvider = null;
	final static String PropertyBeanFilterName = "TaggedLogPropertyBeanFilter";

    public static ObjectMapper buildObjectMapper(){
        final JsonFactory factory = new JsonFactory();
        final ObjectMapper mapper = new ObjectMapper(factory);
        mapper.setFilters(TaggedLogAPIEntity.getFilterProvider());
        return mapper;
    }
}