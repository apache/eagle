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
package org.apache.eagle.alert.engine.spout;

import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.coordination.model.StreamRepartitionMetadata;
import org.apache.eagle.alert.coordination.model.StreamRepartitionStrategy;
import org.apache.eagle.alert.coordination.model.Tuple2StreamConverter;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.serialization.PartitionedEventSerializer;
import org.apache.eagle.alert.engine.serialization.SerializationMetadataProvider;
import org.apache.eagle.alert.utils.StreamIdConversion;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * intercept the message sent from within KafkaSpout and select downstream bolts based on meta-data
 * This is topic based. each topic will have one SpoutOutputCollectorWrapper.
 */
public class SpoutOutputCollectorWrapper extends SpoutOutputCollector implements ISpoutSpecLCM, SerializationMetadataProvider {
    private static final Logger LOG = LoggerFactory.getLogger(SpoutOutputCollectorWrapper.class);

    private final ISpoutOutputCollector delegate;
    private final String topic;
    private final PartitionedEventSerializer serializer;
    private int numOfRouterBolts;

    private volatile List<StreamRepartitionMetadata> streamRepartitionMetadataList;
    private volatile Tuple2StreamConverter converter;
    private CorrelationSpout spout;
    private volatile Map<String, StreamDefinition> sds;

    /**
     * @param delegate        actual SpoutOutputCollector to send data to following bolts
     * @param topic           topic for this KafkaSpout to handle
     * @param numGroupbyBolts bolts following this spout.
     * @param serializer
     */
    public SpoutOutputCollectorWrapper(CorrelationSpout spout,
                                       ISpoutOutputCollector delegate,
                                       String topic,
                                       SpoutSpec spoutSpec,
                                       int numGroupbyBolts,
                                       Map<String, StreamDefinition> sds, PartitionedEventSerializer serializer) {
        super(delegate);
        this.spout = spout;
        this.delegate = delegate;
        this.topic = topic;
        this.streamRepartitionMetadataList = spoutSpec.getStreamRepartitionMetadataMap().get(topic);
        this.converter = new Tuple2StreamConverter(spoutSpec.getTuple2StreamMetadataMap().get(topic));
        this.numOfRouterBolts = numGroupbyBolts;
        this.sds = sds;
        this.serializer = serializer;
    }

    /**
     * How to assert that numTotalGroupbyBolts >= numOfRouterBolts, otherwise
     * there is runtime issue by default, tuple includes 2 fields field 1: topic
     * name field 2: map of key/value.
     */
    @SuppressWarnings("rawtypes")
    @Override
    public List<Integer> emit(List<Object> tuple, Object messageId) {
        if (!sanityCheck()) {
            LOG.error(
                "spout collector for topic {} see monitored metadata invalid, is this data source removed! Trigger message id {} ",
                topic, messageId);
            return null;
        }

        KafkaMessageIdWrapper newMessageId = new KafkaMessageIdWrapper(messageId);
        newMessageId.topic = topic;
        /**
         phase 1: tuple to stream converter
         if this topic multiplexes multiple streams, then retrieve the individual streams.
         */
        List<Object> convertedTuple = converter.convert(tuple);
        if (convertedTuple == null) {
            LOG.warn("source data {} can't be converted to a stream, ignore this message", tuple);
            spout.ack(newMessageId);
            return null;
        }
        Map m = (Map) convertedTuple.get(3);
        Object streamId = convertedTuple.get(1);

        StreamDefinition sd = sds.get(streamId);
        if (sd == null) {
            LOG.warn("StreamDefinition {} is not found within {}, ignore this message", streamId, sds);
            spout.ack(newMessageId);
            return null;
        }

        StreamEvent event = convertToStreamEventByStreamDefinition((Long) convertedTuple.get(2), m, sds.get(streamId));
        /*
            phase 2: stream repartition
        */
        for (StreamRepartitionMetadata md : streamRepartitionMetadataList) {
            // one stream may have multiple group-by strategies, each strategy is for a specific group-by
            for (StreamRepartitionStrategy groupingStrategy : md.groupingStrategies) {
                int hash = 0;
                if (groupingStrategy.getPartition().getType().equals(StreamPartition.Type.GROUPBY)) {
                    hash = getRoutingHashByGroupingStrategy(m, groupingStrategy);
                } else if (groupingStrategy.getPartition().getType().equals(StreamPartition.Type.SHUFFLE)) {
                    hash = Math.abs((int) System.currentTimeMillis());
                }
                int mod = hash % groupingStrategy.numTotalParticipatingRouterBolts;
                // filter out message
                if (mod >= groupingStrategy.startSequence && mod < groupingStrategy.startSequence + numOfRouterBolts) {
                    // framework takes care of field grouping instead of using storm internal field grouping
                    String sid = StreamIdConversion.generateStreamIdBetween(spout.getSpoutName(), spout.getRouteBoltName() + (hash % numOfRouterBolts));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Emitted tuple: {} with message Id: {}, with topic {}, to streamId {}", convertedTuple, messageId, topic, sid);
                    }
                    // send message to StreamRouterBolt
                    PartitionedEvent pEvent = new PartitionedEvent(event, groupingStrategy.partition, hash);
                    if (this.serializer == null) {
                        delegate.emit(sid, Collections.singletonList(pEvent), newMessageId);
                    } else {
                        try {
                            delegate.emit(sid, Collections.singletonList(serializer.serialize(pEvent)), newMessageId);
                        } catch (Exception e) {
                            LOG.error("Failed to serialize {}, this message would be ignored!", pEvent, e);
                            spout.ack(newMessageId);
                        }
                    }
                } else {
                    // ******* short-cut ack ********
                    // we should simply ack those messages which are not processed in this topology because KafkaSpout implementation requires _pending is empty
                    // before moving to next offsets.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Message filtered with mod {} not within range {} and {} for message {}", mod, groupingStrategy.startSequence,
                            groupingStrategy.startSequence + numOfRouterBolts, tuple);
                    }
                    spout.ack(newMessageId);
                }
            }
        }

        return null;
    }

    @SuppressWarnings("rawtypes")
    private int getRoutingHashByGroupingStrategy(Map data, StreamRepartitionStrategy gs) {
        // calculate hash value for values from group-by fields
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();
        for (String groupingField : gs.partition.getColumns()) {
            if (data.get(groupingField) != null) {
                hashCodeBuilder.append(data.get(groupingField));
            } else {
                LOG.warn("Required GroupBy fields {} not found: {}", gs.partition.getColumns(), data);
            }
        }
        int hash = hashCodeBuilder.toHashCode();
        hash = Math.abs(hash);
        return hash;
    }

    private boolean sanityCheck() {
        boolean isOk = true;
        if (streamRepartitionMetadataList == null) {
            LOG.error("streamRepartitionMetadataList is null!");
            isOk = false;
        }
        if (converter == null) {
            LOG.error("tuple2StreamMetadata is null!");
            isOk = false;
        }
        return isOk;
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    private StreamEvent convertToStreamEventByStreamDefinition(long timestamp, Map m, StreamDefinition sd) {
        return StreamEvent.builder().timestamep(timestamp).attributes(m, sd).build();
    }

    /**
     * SpoutSpec may be changed, this class will respond to changes on tuple2StreamMetadataMap and streamRepartitionMetadataMap.
     *
     * @param spoutSpec
     * @param sds
     */
    @Override
    public void update(SpoutSpec spoutSpec, Map<String, StreamDefinition> sds) {
        this.streamRepartitionMetadataList = spoutSpec.getStreamRepartitionMetadataMap().get(topic);
        this.converter = new Tuple2StreamConverter(spoutSpec.getTuple2StreamMetadataMap().get(topic));
        this.sds = sds;
    }

    @Override
    public StreamDefinition getStreamDefinition(String streamId) {
        return this.sds.get(streamId);
    }
}