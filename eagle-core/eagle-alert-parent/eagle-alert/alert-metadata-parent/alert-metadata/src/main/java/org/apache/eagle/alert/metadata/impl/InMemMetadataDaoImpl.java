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
package org.apache.eagle.alert.metadata.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.PublishmentType;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.metadata.resource.IMetadataDao;
import org.apache.eagle.alert.metadata.resource.Models;
import org.apache.eagle.alert.metadata.resource.OpResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

/**
 * In memory service for simple service start. Make all service API as
 * synchronized.
 * 
 * @since Apr 11, 2016
 *
 */
public class InMemMetadataDaoImpl implements IMetadataDao {

    private static final Logger LOG = LoggerFactory.getLogger(InMemMetadataDaoImpl.class);

    private List<StreamingCluster> clusters = new ArrayList<StreamingCluster>();
    private List<StreamDefinition> schemas = new ArrayList<StreamDefinition>();
    private List<Kafka2TupleMetadata> datasources = new ArrayList<Kafka2TupleMetadata>();
    private List<PolicyDefinition> policies = new ArrayList<PolicyDefinition>();
    private List<Publishment> publishments = new ArrayList<Publishment>();
    private List<PublishmentType> publishmentTypes = new ArrayList<PublishmentType>();
    private volatile int maxScheduleState = 100;
    private SortedMap<String, ScheduleState> scheduleStates = new TreeMap<String, ScheduleState>();
    private List<PolicyAssignment> assignments = new ArrayList<PolicyAssignment>();
    private List<Topology> topologies = new ArrayList<Topology>();
    
    public InMemMetadataDaoImpl(Config config) {
    }

    @Override
    public synchronized List<StreamingCluster> listClusters() {
        return clusters;
    }

    @Override
    public OpResult addCluster(final StreamingCluster cluster) {
        return addOrReplace(clusters, cluster);
    }

    private synchronized <T> OpResult addOrReplace(List<T> clusters, T paramT) {
        Optional<T> scOp = clusters.stream().filter(new Predicate<T>() {
            @Override
            public boolean test(T t) {
                if (getKey(t).equalsIgnoreCase(getKey(paramT))) {
                    return true;
                }
                return false;
            }
        }).findFirst();

        OpResult result = new OpResult();
        // replace
        if (scOp.isPresent()) {
            clusters.remove(scOp.get());
            result.message = "replace the old one!";
        } else {
            result.message = "created new config!";
        }
        result.code = 200;
        clusters.add(paramT);
        return result;
    }

    public static <T> String getKey(T t) {
        if (t instanceof StreamDefinition) {
            return ((StreamDefinition) t).getStreamId();
        }
        if (t instanceof PublishmentType) {
            return ((PublishmentType) t).getType();
        }

        try {
            Method m = t.getClass().getMethod("getName");
            return (String) m.invoke(t);
        } catch (NoSuchMethodException | SecurityException | InvocationTargetException | IllegalAccessException
                | IllegalArgumentException e) {
            LOG.error(" getName not found on given class :" + t.getClass().getName());
        }
        throw new RuntimeException(String.format("no getName() found on target class %s for matching", t.getClass()
                .getName()));
    }

    @SuppressWarnings("unchecked")
    private synchronized <T> OpResult remove(List<T> clusters, String id) {
        T[] matched = (T[]) clusters.stream().filter(new Predicate<T>() {

            @Override
            public boolean test(T t) {
                if (getKey(t).equalsIgnoreCase(id)) {
                    return true;
                }
                return false;
            }
        }).toArray();

        OpResult result = new OpResult();
        result.code = 200;
        if (clusters.removeAll(Arrays.asList(matched))) {
            result.message = "removed configuration item succeed";
        } else {
            result.message = "no configuration item removed";
        }
        return result;
    }

    @Override
    public OpResult removeCluster(final String clusterId) {
        return remove(clusters, clusterId);
    }

    @Override
    public synchronized List<StreamDefinition> listStreams() {
        return schemas;
    }

    @Override
    public OpResult createStream(StreamDefinition stream) {
        return addOrReplace(schemas, stream);
    }

    @Override
    public OpResult removeStream(String streamId) {
        return remove(schemas, streamId);
    }

    @Override
    public synchronized List<Kafka2TupleMetadata> listDataSources() {
        return datasources;
    }

    @Override
    public OpResult addDataSource(Kafka2TupleMetadata dataSource) {
        return addOrReplace(datasources, dataSource);
    }

    @Override
    public OpResult removeDataSource(String datasourceId) {
        return remove(datasources, datasourceId);
    }

    @Override
    public synchronized List<PolicyDefinition> listPolicies() {
        return policies;
    }

    @Override
    public OpResult addPolicy(PolicyDefinition policy) {
        return addOrReplace(policies, policy);
    }

    @Override
    public OpResult removePolicy(String policyId) {
        return remove(policies, policyId);
    }

    @Override
    public synchronized List<Publishment> listPublishment() {
        return publishments;
    }

    @Override
    public OpResult addPublishment(Publishment publishment) {
        return addOrReplace(publishments, publishment);
    }

    @Override
    public OpResult removePublishment(String pubId) {
        return remove(publishments, pubId);
    }

    @Override
    public List<PublishmentType> listPublishmentType() {
        return publishmentTypes;
    }

    @Override
    public OpResult addPublishmentType(PublishmentType publishmentType) {
        return addOrReplace(publishmentTypes, publishmentType);
    }

    @Override
    public OpResult removePublishmentType(String pubType) {
        return remove(publishmentTypes, pubType);
    }

    @Override
    public ScheduleState getScheduleState(String versionId) {
        return scheduleStates.get(versionId);
    }

    @Override
    public OpResult addScheduleState(ScheduleState state) {
        // FIXME : might concurrent issue
        String toRemove = null;
        if (scheduleStates.size() > maxScheduleState) {
            toRemove = scheduleStates.firstKey();
        }
        scheduleStates.put(state.getVersion(), state);
        if (toRemove != null) {
            scheduleStates.remove(toRemove);
        }

        OpResult result = new OpResult();
        result.code = 200;
        result.message = "OK";
        return result;
    }

    @Override
    public ScheduleState getScheduleState() {
        if (scheduleStates.size() > 0) {
            return scheduleStates.get(scheduleStates.lastKey());
        }
        return null;
    }

    @Override
    public List<PolicyAssignment> listAssignments() {
        return assignments;
    }

    @Override
    public OpResult addAssignment(PolicyAssignment assignment) {
        OpResult result = new OpResult();
        result.code = 200;
        result.message = "OK";
        assignments.add(assignment);
        return result;
    }

    @Override
    public List<Topology> listTopologies() {
        return topologies;
    }

    @Override
    public OpResult addTopology(Topology t) {
        return addOrReplace(topologies, t);
    }

    @Override
    public OpResult removeTopology(String topologyName) {
        return remove(topologies, topologyName);
    }

    @Override
    public OpResult clear() {
        this.assignments.clear();
        this.clusters.clear();
        this.datasources.clear();
        this.policies.clear();
        this.publishments.clear();
        this.scheduleStates.clear();
        this.schemas.clear();
        this.topologies.clear();
        OpResult result = new OpResult();
        result.code = 200;
        result.message = "OK";
        return result;
    }

    @Override
    public Models export() {
        Models models = new Models();
        models.assignments.addAll(this.assignments);
        models.clusters.addAll(this.clusters);
        models.datasources.addAll(this.datasources);
        models.policies.addAll(this.policies);
        models.publishments.addAll(this.publishments);
        models.scheduleStates.putAll(this.scheduleStates);
        models.schemas.addAll(this.schemas);
        models.topologies.addAll(this.topologies);
        return models;
    }

    @Override
    public OpResult importModels(Models models) {
        clear();
        this.assignments.addAll(models.assignments);
        this.clusters.addAll(models.clusters);
        this.datasources.addAll(models.datasources);
        this.policies.addAll(models.policies);
        this.publishments.addAll(models.publishments);
        this.scheduleStates.putAll(models.scheduleStates);
        this.schemas.addAll(models.schemas);
        this.topologies.addAll(models.topologies);
        OpResult result = new OpResult();
        result.code = 200;
        result.message = "OK";
        return result;
    }

}
