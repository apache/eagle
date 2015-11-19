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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.repo.EntityRepositoryScanner;
import org.mockito.cglib.beans.BeanGenerator;
import org.mockito.cglib.core.NamingPolicy;
import org.mockito.cglib.core.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * static initialization of all registered entities. As of now, dynamic registration is not supported
 */
public class EntityDefinitionManager {
	private static final Logger LOG = LoggerFactory.getLogger(EntityDefinitionManager.class);
	private static volatile boolean initialized = false;
	/**
	 * using concurrent hashmap is due to the fact that entity can be registered any time from any thread
	 */
	private static Map<String, EntityDefinition> entityServiceMap = new ConcurrentHashMap<String, EntityDefinition>();
	private static Map<Class<? extends TaggedLogAPIEntity>, EntityDefinition> classMap = new ConcurrentHashMap<Class<? extends TaggedLogAPIEntity>, EntityDefinition>();
	private static Map<Class<?>, EntitySerDeser<?>> _serDeserMap = new ConcurrentHashMap<Class<?>, EntitySerDeser<?>>(); 
	private static Map<Class<?>, Integer> _serDeserClassIDMap = new ConcurrentHashMap<Class<?>, Integer>(); 
	private static Map<Integer, Class<?>> _serIDDeserClassMap = new ConcurrentHashMap<Integer, Class<?>>(); 
	private static Map<String, Map<Integer, EntityDefinition>> entityPrefixMap = new ConcurrentHashMap<String, Map<Integer, EntityDefinition>>();
	private static Map<String, Map<Integer, IndexDefinition>> indexPrefixMap = new ConcurrentHashMap<String, Map<Integer, IndexDefinition>>();

	static{
		int id = 0;
		_serDeserMap.put(NullObject.class, new NullSerDeser());
		_serIDDeserClassMap.put(id, NullObject.class);
		_serDeserClassIDMap.put(NullObject.class, id++);
		
		_serDeserMap.put(String.class, new StringSerDeser());
		_serIDDeserClassMap.put(id, String.class);
		_serDeserClassIDMap.put(String.class, id++);
		
		_serDeserMap.put(long.class, new LongSerDeser());
		_serIDDeserClassMap.put(id, long.class);
		_serDeserClassIDMap.put(long.class, id++);
		
		_serDeserMap.put(Long.class, new LongSerDeser());
		_serIDDeserClassMap.put(id, Long.class);
		_serDeserClassIDMap.put(Long.class, id++);
		
		_serDeserMap.put(int.class, new IntSerDeser());
		_serIDDeserClassMap.put(id, int.class);
		_serDeserClassIDMap.put(int.class, id++);
		
		_serDeserMap.put(Integer.class, new IntSerDeser());
		_serIDDeserClassMap.put(id, Integer.class);
		_serDeserClassIDMap.put(Integer.class, id++);
		
		_serDeserMap.put(Double.class, new DoubleSerDeser());
		_serIDDeserClassMap.put(id, Double.class);
		_serDeserClassIDMap.put(Double.class, id++);
		
		_serDeserMap.put(double.class, new DoubleSerDeser());
		_serIDDeserClassMap.put(id, double.class);
		_serDeserClassIDMap.put(double.class, id++);
		
		_serDeserMap.put(int[].class, new IntArraySerDeser());
		_serIDDeserClassMap.put(id, int[].class);
		_serDeserClassIDMap.put(int[].class, id++);
		
		_serDeserMap.put(double[].class, new DoubleArraySerDeser());
		_serIDDeserClassMap.put(id, double[].class);
		_serDeserClassIDMap.put(double[].class, id++);

		_serDeserMap.put(double[][].class, new Double2DArraySerDeser());
		_serIDDeserClassMap.put(id, double[][].class);
		_serDeserClassIDMap.put(double[][].class, id++);
		
		_serDeserMap.put(Boolean.class, new BooleanSerDeser());
		_serIDDeserClassMap.put(id, Boolean.class);
		_serDeserClassIDMap.put(Boolean.class, id++);
		
		_serDeserMap.put(boolean.class, new BooleanSerDeser());
		_serIDDeserClassMap.put(id, boolean.class);
		_serDeserClassIDMap.put(boolean.class, id++);
		
		_serDeserMap.put(String[].class, new StringArraySerDeser());
		_serIDDeserClassMap.put(id, String[].class);
		_serDeserClassIDMap.put(String[].class, id++);
		
		_serDeserMap.put(Map.class, new MapSerDeser());
		_serIDDeserClassMap.put(id, Map.class);
		_serDeserClassIDMap.put(Map.class, id++);
		
		_serDeserMap.put(List.class, new ListSerDeser());
		_serIDDeserClassMap.put(id, List.class);
		_serDeserClassIDMap.put(List.class, id++);
	}
	
	

	@SuppressWarnings("rawtypes")
	public static EntitySerDeser getSerDeser(Class<?> clazz){
		return _serDeserMap.get(clazz);
	}

	/**
	 * Get internal ID by the predefined registered class
	 * @param clazz original for serialization/deserialization 
	 * @return the internal id if the input class has been registered, otherwise return -1
	 */
	public static int getIDBySerDerClass(Class<?> clazz) {
		final Integer id = _serDeserClassIDMap.get(clazz);
		if (id == null) {
			return -1;
		}
		return id;
	}
	

	/**
	 * Get the predefined registered class by internal ID
	 * @param id the internal class ID
	 * @return the predefined registered class, if the class hasn't been registered, return null
	 */
	public static Class<?> getClassByID(int id) {
		return _serIDDeserClassMap.get(id);
	}
	
	/**
	 * it is allowed that user can register their own entity
	 * @param clazz entity class
	 * @throws IllegalArgumentException
	 */
	public static void registerEntity(Class<? extends TaggedLogAPIEntity> clazz) throws IllegalArgumentException{
		registerEntity(createEntityDefinition(clazz));
	}
	
	/**
	 * it is allowed that user can register their own entity
	 * @deprecated This API is deprecated since we need to use Service annotation to define service name for entities
	 * @param serviceName entity service name
	 * @param clazz entity class
	 * @throws IllegalArgumentException
	 * 
	 */
    @Deprecated
	public static void registerEntity(String serviceName, Class<? extends TaggedLogAPIEntity> clazz) throws IllegalArgumentException{
		registerEntity(serviceName, createEntityDefinition(clazz));
	}
	
	/**
	 * it is allowed that user can register their own entity definition
	 * @param entityDef entity definition
	 * @throws IllegalArgumentException
	 */
	public static void registerEntity(EntityDefinition entityDef) {
		registerEntity(entityDef.getService(), entityDef);
	}
	
	/**
	 * it is allowed that user can register their own entity definition
	 * @deprecated This API is deprecated since we need to use Service annotation to define service name for entities. 
	 * 
	 * @param entityDef entity definition
	 * @throws IllegalArgumentException
	 */
	public static void registerEntity(String serviceName, EntityDefinition entityDef) {
		final String table = entityDef.getTable();
		if (entityServiceMap.containsKey(serviceName)) {
			final EntityDefinition existing = entityServiceMap.get(serviceName);
			if (entityDef.getClass().equals(existing.getClass())) {
				return;
			}
			throw new IllegalArgumentException("Service " + serviceName + " has already been registered by " + existing.getClass().getName() + ", so class " + entityDef.getClass() + " can NOT be registered");
		}
		synchronized (EntityDefinitionManager.class) {
			checkPrefix(entityDef);
			entityServiceMap.put(serviceName, entityDef);
			Map<Integer, EntityDefinition> entityHashMap = entityPrefixMap.get(table);
			if (entityHashMap == null) {
				entityHashMap = new ConcurrentHashMap<Integer, EntityDefinition>();
				entityPrefixMap.put(table, entityHashMap);
			}
			entityHashMap.put(entityDef.getPrefix().hashCode(), entityDef);
			final IndexDefinition[] indexes = entityDef.getIndexes();
			if (indexes != null) {
				for (IndexDefinition index : indexes) {
					Map<Integer, IndexDefinition> indexHashMap = indexPrefixMap.get(table);
					if (indexHashMap == null) {
						indexHashMap = new ConcurrentHashMap<Integer, IndexDefinition>();
						indexPrefixMap.put(table, indexHashMap);
					}
					indexHashMap.put(index.getIndexPrefix().hashCode(), index);
				}
			}
			classMap.put(entityDef.getEntityClass(), entityDef);
		}
        if(LOG.isDebugEnabled()) {
            LOG.debug(entityDef.getEntityClass().getSimpleName() + " entity registered successfully, table name: " + entityDef.getTable() +
					", prefix: " + entityDef.getPrefix() + ", service: " + serviceName + ", CF: " + entityDef.getColumnFamily());
        }else{
            LOG.info(String.format("Registered %s (%s)", entityDef.getEntityClass().getSimpleName(), serviceName));
        }
	}

	private static void checkPrefix(EntityDefinition entityDef) {
		final Integer entityPrefixHashcode = entityDef.getPrefix().hashCode();
		if (entityPrefixMap.containsKey(entityDef.getTable())) {
			final Map<Integer, EntityDefinition> entityHashMap = entityPrefixMap.get(entityDef.getTable());
			if (entityHashMap.containsKey(entityPrefixHashcode) && (!entityDef.equals(entityHashMap.get(entityPrefixHashcode)))) {
				throw new IllegalArgumentException("Failed to register entity " + entityDef.getClass().getName() + ", because of the prefix hash code conflict! The entity prefix " + entityDef.getPrefix() + " has already been registered by entity service " + entityHashMap.get(entityPrefixHashcode).getService());
			}
			final IndexDefinition[] indexes = entityDef.getIndexes();
			if (indexes != null) {
				for (IndexDefinition index : indexes) {
					final Integer indexPrefixHashcode = index.getIndexPrefix().hashCode();
					if (entityHashMap.containsKey(indexPrefixHashcode)) {
						throw new IllegalArgumentException("Failed to register entity " + entityDef.getClass().getName() + ", because of the prefix hash code conflict! The index prefix " + index.getIndexPrefix() + " has already been registered by entity " + entityHashMap.get(indexPrefixHashcode).getService());
					}
					final Map<Integer, IndexDefinition> indexHashMap = indexPrefixMap.get(entityDef.getTable());
					if (indexHashMap != null && indexHashMap.containsKey(indexPrefixHashcode) && (!index.equals(indexHashMap.get(indexPrefixHashcode)))) {
						throw new IllegalArgumentException("Failed to register entity " + entityDef.getClass().getName() + ", because of the prefix hash code conflict! The index prefix " + index.getIndexPrefix() + " has already been registered by entity " + indexHashMap.get(indexPrefixHashcode).getEntityDefinition().getService());
					}
				}
			}
		}
	}
	
	/**
	 * Get entity definition by name
	 * @param serviceName
	 * @return
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static EntityDefinition getEntityByServiceName(String serviceName) throws InstantiationException, IllegalAccessException{
		checkInit();
		return entityServiceMap.get(serviceName);
	}
	
	public static EntityDefinition getEntityDefinitionByEntityClass(Class<? extends TaggedLogAPIEntity> clazz) throws InstantiationException, IllegalAccessException {
		checkInit();
		return classMap.get(clazz);
	}

	private static void checkInit() throws InstantiationException, IllegalAccessException {
		if (!initialized) {
			synchronized (EntityDefinitionManager.class) {
				if (!initialized) {
					EntityRepositoryScanner.scan();
					initialized = true;
				}
			}
		}
	}

	/**
	 * User can register their own field SerDeser
	 * @param clazz class of the the SerDeser 
	 * @param entitySerDeser entity or field SerDeser
	 * @throws IllegalArgumentException
	 */
	public static void registerSerDeser(Class<?> clazz, EntitySerDeser<?> entitySerDeser) {
		_serDeserMap.put(clazz, entitySerDeser);
	}

	/**
	 * Check whether the entity class is time series, false by default
	 * @param clazz
	 * @return
	 */
	public static boolean isTimeSeries(Class<? extends TaggedLogAPIEntity> clazz){
		TimeSeries ts = clazz.getAnnotation(TimeSeries.class);
		return ts != null && ts.value();
	}

	@SuppressWarnings("unchecked")
	public static EntityDefinition createEntityDefinition(Class<? extends TaggedLogAPIEntity> cls) {
		
		final EntityDefinition ed = new EntityDefinition();

		ed.setEntityClass(cls);
		// parse cls' annotations
		Table table = cls.getAnnotation(Table.class);
		if(table == null || table.value().isEmpty()){
			throw new IllegalArgumentException("Entity class must have a non-empty table name annotated with @Table");
		}
		String tableName = table.value();
		if(EagleConfigFactory.load().isTableNamePrefixedWithEnvironment()){
			tableName = EagleConfigFactory.load().getEnv() + "_" + tableName;
		}
		ed.setTable(tableName);
		
		ColumnFamily family = cls.getAnnotation(ColumnFamily.class);
		if(family == null || family.value().isEmpty()){
			throw new IllegalArgumentException("Entity class must have a non-empty column family name annotated with @ColumnFamily");
		}
		ed.setColumnFamily(family.value());
		
		Prefix prefix = cls.getAnnotation(Prefix.class);
		if(prefix == null || prefix.value().isEmpty()){
			throw new IllegalArgumentException("Entity class must have a non-empty prefix name annotated with @Prefix");
		}
		ed.setPrefix(prefix.value());
		
		TimeSeries ts = cls.getAnnotation(TimeSeries.class);
		if(ts == null){
			throw new IllegalArgumentException("Entity class must have a non-empty timeseries name annotated with @TimeSeries");
		}
		ed.setTimeSeries(ts.value());
		
		Service service = cls.getAnnotation(Service.class);
		if(service == null || service.value().isEmpty()){
			ed.setService(cls.getSimpleName());
		} else {
			ed.setService(service.value());
		}

		Metric m = cls.getAnnotation(Metric.class);
		Map<String, Class<?>> dynamicFieldTypes = new HashMap<String, Class<?>>();
		if(m != null){
			// metric has to be timeseries
			if(!ts.value()){
				throw new IllegalArgumentException("Metric entity must be time series as well");
			}
			MetricDefinition md = new MetricDefinition();
			md.setInterval(m.interval());
			ed.setMetricDefinition(md);
		}

		java.lang.reflect.Field[] fields = cls.getDeclaredFields();
		for(java.lang.reflect.Field f : fields){
			Column column = f.getAnnotation(Column.class); 
			if(column == null || column.value().isEmpty()){
				continue;
			}
			Class<?> fldCls = f.getType();
			// intrusive check field type for metric entity
			checkFieldTypeForMetric(ed.getMetricDefinition(), f.getName(), fldCls, dynamicFieldTypes);
			Qualifier q = new Qualifier();
			q.setDisplayName(f.getName());
			q.setQualifierName(column.value());
			EntitySerDeser<?> serDeser = _serDeserMap.get(fldCls); 
			if(serDeser == null){
				throw new IllegalArgumentException(fldCls.getName() + " in field " + f.getName() + 
						" of entity " + cls.getSimpleName() + " has no serializer associated ");
			} else {
				q.setSerDeser((EntitySerDeser<Object>)serDeser);
			}
			ed.getQualifierNameMap().put(q.getQualifierName(), q);
			ed.getDisplayNameMap().put(q.getDisplayName(), q);
			// TODO: should refine rules, consider fields like "hCol", getter method should be gethCol() according to org.apache.commons.beanutils.PropertyUtils
			final String propertyName = f.getName().substring(0,1).toUpperCase() + f.getName().substring(1);
			String getterName = "get" + propertyName;
			try {
				Method method = cls.getMethod(getterName);
				ed.getQualifierGetterMap().put(f.getName(), method);
			} catch (Exception e) {
				// Check if the type is boolean
				getterName = "is" + propertyName;
				try {
					Method method = cls.getMethod(getterName);
					ed.getQualifierGetterMap().put(f.getName(), method);
				} catch (Exception e1) {
					throw new IllegalArgumentException("Field " + f.getName() + " hasn't defined valid getter method: " + getterName, e);
				}
			}
			if(LOG.isDebugEnabled()) LOG.debug("Field registered " + q);
		}

		// TODO: Lazy create because not used at all
		// dynamically create bean class
		if(ed.getMetricDefinition() != null){
			Class<?> metricCls = createDynamicClassForMetric(cls.getName()+"_SingleTimestamp", dynamicFieldTypes);
			ed.getMetricDefinition().setSingleTimestampEntityClass(metricCls);
		}
		
		final Partition partition = cls.getAnnotation(Partition.class);
		if (partition != null) {
			final String[] partitions = partition.value();
			ed.setPartitions(partitions);
			// Check if partition fields are all tag fields. Partition field can't be column field, must be tag field.
			for (String part : partitions) {
				if (!ed.isTag(part)) {
					throw new IllegalArgumentException("Partition field can't be column field, must be tag field. "
							+ "Partition name: " + part);
				}
			}
		}
		
		final Indexes indexes = cls.getAnnotation(Indexes.class);
		if (indexes != null) {
			final Index[] inds = indexes.value();
			final IndexDefinition[] indexDefinitions = new IndexDefinition[inds.length];
			for (int i = 0; i < inds.length; ++i) {
				final Index ind = inds[i];
				indexDefinitions[i] = new IndexDefinition(ed, ind);
			}
			ed.setIndexes(indexDefinitions);
		}
		
		final ServicePath path = cls.getAnnotation(ServicePath.class);
		if (path != null) {
			if (path.path() != null && (!path.path().isEmpty())) {
				ed.setServiceCreationPath(path.path());
			}
		}
		
		return ed;
	}
	
	private static void checkFieldTypeForMetric(MetricDefinition md, String fieldName, Object fldCls, Map<String, Class<?>> dynamicFieldTypes){
		if(md != null){
			if(fldCls.equals(int[].class)){
				dynamicFieldTypes.put(fieldName, int.class);
				return;
			}else if(fldCls.equals(long[].class)){
				dynamicFieldTypes.put(fieldName, long.class);
				return;
			}else if(fldCls.equals(double[].class)){
				dynamicFieldTypes.put(fieldName, double.class);
				return;
			}
			throw new IllegalArgumentException("Fields for metric entity must be one of int[], long[] or double[]");
		}
	}
	
	private static Class<?> createDynamicClassForMetric(final String className, Map<String, Class<?>> dynamicFieldTypes){
		BeanGenerator beanGenerator = new BeanGenerator();
		beanGenerator.setNamingPolicy(new NamingPolicy(){
	        @Override 
	        public String getClassName(String prefix,String source, Object key, Predicate names){
	            return className;
	        }});
	    BeanGenerator.addProperties(beanGenerator, dynamicFieldTypes);
	    beanGenerator.setSuperclass(TaggedLogAPIEntity.class);
	    return (Class<?>) beanGenerator.createClass();
	}
	
	public static Map<String, EntityDefinition> entities() throws Exception{
		checkInit();
		return entityServiceMap;
	}
}
