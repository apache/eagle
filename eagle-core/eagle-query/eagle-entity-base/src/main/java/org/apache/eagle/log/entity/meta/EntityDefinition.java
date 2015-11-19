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
package org.apache.eagle.log.entity.meta;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericMetricShadowEntity;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * This object should be regarded as read-only metadata for an entity as it will be shared across all entity object
 * with the same entity name, so don't try to set different values for any of the fields, 
 * otherwise it's not thread safe
 */
public class EntityDefinition implements Writable{
	private final static Logger LOG = LoggerFactory.getLogger(EntityDefinition.class);

	private Class<? extends TaggedLogAPIEntity> entityClass;
	private String table;
	private String columnFamily;
	// TODO prefix be within search/get condition instead of entity definition. Topology entity should have pre-defined prefix. 
	private String prefix;
	private String service;
	private String serviceCreationPath;
	private String serviceDeletionPath;
	private String[] partitions;
	private Map<String, Qualifier> displayNameMap = new HashMap<String, Qualifier>();
	private Map<String, Qualifier> qualifierNameMap = new HashMap<String, Qualifier>();
	private Map<String, Method> qualifierGetterMap = new HashMap<String, Method>();
	private boolean isTimeSeries;
	private MetricDefinition metricDefinition;
	private IndexDefinition[] indexes;
	

	public EntityDefinition(){}
	
	public MetricDefinition getMetricDefinition() {
		return metricDefinition;
	}
	public void setMetricDefinition(MetricDefinition metricDefinition) {
		this.metricDefinition = metricDefinition;
	}
	public boolean isTimeSeries() {
		return isTimeSeries;
	}
	public void setTimeSeries(boolean isTimeSeries) {
		this.isTimeSeries = isTimeSeries;
	}
	public String getColumnFamily() {
		return columnFamily;
	}
	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}
	public Class<? extends TaggedLogAPIEntity> getEntityClass() {
		return entityClass;
	}
	public void setEntityClass(Class<? extends TaggedLogAPIEntity> entityClass) {
		this.entityClass = entityClass;
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public Map<String, Qualifier> getDisplayNameMap() {
		return displayNameMap;
	}
	public void setDisplayNameMap(Map<String, Qualifier> displayNameMap) {
		this.displayNameMap = displayNameMap;
	}
	public Map<String, Qualifier> getQualifierNameMap() {
		return qualifierNameMap;
	}
	public void setQualifierNameMap(Map<String, Qualifier> qualifierNameMap) {
		this.qualifierNameMap = qualifierNameMap;
	}
	public String getPrefix() {
		return prefix;
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	public String getService() {
		return service;
	}
	public void setService(String service) {
		this.service = service;
	}
	public String getServiceCreationPath() {
		return serviceCreationPath;
	}
	public void setServiceCreationPath(String serviceCreationPath) {
		this.serviceCreationPath = serviceCreationPath;
	}
	public String getServiceDeletionPath() {
		return serviceDeletionPath;
	}
	public void setServiceDeletionPath(String serviceDeletionPath) {
		this.serviceDeletionPath = serviceDeletionPath;
	}
	public String[] getPartitions() {
		return partitions;
	}
	public void setPartitions(String[] partitions) {
		this.partitions = partitions;
	}
	public IndexDefinition[] getIndexes() {
		return indexes;
	}
	public void setIndexes(IndexDefinition[] indexes) {
		this.indexes = indexes;
	}
	public Map<String, Method> getQualifierGetterMap() {
		return qualifierGetterMap;
	}
	public void setQualifierGetterMap(Map<String, Method> qualifierGetterMap) {
		this.qualifierGetterMap = qualifierGetterMap;
	}
//	public Map<String,String> getQualifierDisplayNameMap(){
//		Map<String,String> qualifierDisplayNameMap = new HashMap<String, String>();
//		for(Map.Entry<String,Qualifier> entry: qualifierNameMap.entrySet()){
//			qualifierDisplayNameMap.put(entry.getKey(),entry.getValue().getDisplayName());
//		}
//		return qualifierDisplayNameMap;
//	}
	
	/**
	 * a filed is a tag when this field is neither in qualifierNameMap nor in displayNameMap
	 * @param field
	 * @return
	 */
	public boolean isTag(String field){
		return (qualifierNameMap.get(field) == null && displayNameMap.get(field) == null);
//		return (qualifierNameMap.get(field) == null);
	}

	/**
	 * Check if the specified field is a partition tag field
	 */
	public boolean isPartitionTag(String field) {
		if (partitions == null || (!isTag(field))) {
			return false;
		}
		for (String partition : partitions) {
			if (partition.equals(field)) {
				return true;
			}
		}
		return false;

	}
	
	public Object getValue(TaggedLogAPIEntity entity, String field) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		if (!entityClass.equals(entity.getClass())) {
			if ((entityClass.equals(GenericMetricEntity.class) && entity.getClass().equals(GenericMetricShadowEntity.class))) {
				GenericMetricShadowEntity e = (GenericMetricShadowEntity)entity;
				return e.getValue();
			} else {
				throw new IllegalArgumentException("Invalid entity type: " + entity.getClass().getSimpleName());
			}
		}
		final Method m = qualifierGetterMap.get(field);
		if (m == null) {
			// The field is a tag
			if (entity.getTags() != null) {
				return entity.getTags().get(field);
			}
		}
		if (m != null) {
			return m.invoke(entity);
		}
		return null;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(entityClass.getName());
		out.writeUTF(table);
		out.writeUTF(columnFamily);
		out.writeUTF(prefix);
		out.writeUTF(service);

		int partitionsLen = 0;
		if(partitions != null) partitionsLen =partitions.length;
		out.writeInt(partitionsLen);
		for (int i = 0; i < partitionsLen; i++) {
			out.writeUTF(partitions[i]);
		}

		int displayNameMapSize = displayNameMap.size();
		out.writeInt(displayNameMapSize);
		for(Map.Entry<String,Qualifier> entry: displayNameMap.entrySet()){
			out.writeUTF(entry.getKey());
			entry.getValue().write(out);
		}

		int qualifierNameMapSize = qualifierNameMap.size();
		out.writeInt(qualifierNameMapSize);
		for(Map.Entry<String,Qualifier> entry: qualifierNameMap.entrySet()){
			out.writeUTF(entry.getKey());
			entry.getValue().write(out);
		}

		// TODO: write qualifierGetterMap
		out.writeBoolean(isTimeSeries);

		boolean hasMetricDefinition = metricDefinition != null;
		out.writeBoolean(hasMetricDefinition);
		if(hasMetricDefinition) {
			// write MetricDefinition
			metricDefinition.write(out);
		}

		// TODO: write indexes
	}


	public void setEntityDefinition(EntityDefinition ed){
		this.entityClass = ed.getEntityClass();
		this.table = ed.getTable();
		this.columnFamily = ed.getColumnFamily();
		this.prefix = ed.getPrefix();
		this.service = ed.getService();
		this.partitions = ed.getPartitions();
		this.displayNameMap = ed.getDisplayNameMap();
		this.qualifierGetterMap = ed.getQualifierGetterMap();
        this.qualifierNameMap = ed.getQualifierNameMap();
		this.isTimeSeries = ed.isTimeSeries();
		this.metricDefinition = ed.metricDefinition;
		this.indexes = ed.getIndexes();
	}

	//////////////////////////////////////////////
	// 	TODO: Cache object for reading in region side
	//////////////////////////////////////////////
	//	private final static Map<String,EntityDefinition> _classEntityDefinitionCache = new HashMap<String, EntityDefinition>();

	@Override
	public void readFields(DataInput in) throws IOException {
		String entityClassName = in.readUTF();
//		EntityDefinition _cached = _classEntityDefinitionCache.get(entityClassName);
//		if(_cached !=null){
//			setEntityDefinition(_cached);
//			LOG.info("Got cached definition for entity: "+entityClassName);
//			return;
//		}
		if(LOG.isDebugEnabled()) LOG.debug("Reading EntityDefinition entity: "+entityClassName);
		try {
			entityClass = (Class<? extends TaggedLogAPIEntity>) Class.forName(entityClassName);
		} catch (Exception e) {
			// ignore
		}
		table = in.readUTF();
		columnFamily = in.readUTF();
		prefix = in.readUTF();
		service = in.readUTF();

		int partitionsLen = in.readInt();
		partitions = new String[partitionsLen];
		for (int i = 0; i < partitionsLen; i++) {
			partitions[i] = in.readUTF();
		}
		int displayNameMapSize = in.readInt();
		for(int i=0;i<displayNameMapSize;i++){
			String key = in.readUTF();
			Qualifier value = new Qualifier();
			value.readFields(in);
			displayNameMap.put(key,value);
		}
		int qualifierNameMapSize = in.readInt();
		for(int i=0;i<qualifierNameMapSize;i++){
			String key = in.readUTF();
			Qualifier value = new Qualifier();
			value.readFields(in);
			qualifierNameMap.put(key,value);
		}
		// TODO: readFields qualifierGetterMap
		isTimeSeries = in.readBoolean();

		// readFields MetricDefinition
		boolean hasMetricDefinition = in.readBoolean();
		if(hasMetricDefinition) {
			if(LOG.isDebugEnabled()) LOG.debug("reading metricDefinition");
			metricDefinition = new MetricDefinition();
			metricDefinition.readFields(in);
		}
		// TODO: readFields indexes
//		_classEntityDefinitionCache.put(entityClassName,this);
	}
}