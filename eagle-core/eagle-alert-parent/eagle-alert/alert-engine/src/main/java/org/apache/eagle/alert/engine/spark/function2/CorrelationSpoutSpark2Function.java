package org.apache.eagle.alert.engine.spark.function2;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.alert.coordination.model.*;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.serialization.SerializationMetadataProvider;
import org.apache.eagle.alert.engine.spark.function.CorrelationSpoutSparkFunction;
import org.apache.eagle.alert.service.SpecMetadataServiceClientImpl;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class CorrelationSpoutSpark2Function implements FlatMapFunction<String, Tuple2<Integer, PartitionedEvent>>, SerializationMetadataProvider {


    private static final long serialVersionUID = -5281723341236671580L;
    private static final Logger LOG = LoggerFactory.getLogger(CorrelationSpoutSparkFunction.class);

    private int numOfRouterBolts;
    //  private String topic = "";
    private SpoutSpec spoutSpec;
    private Map<String, StreamDefinition> sds;
    private Config config;

    public CorrelationSpoutSpark2Function(int numOfRouter, SpoutSpec spoutSpec, Map<String, StreamDefinition> sds) {
        this.spoutSpec = spoutSpec;
        this.sds = sds;
        this.numOfRouterBolts = numOfRouter;
    }

    public CorrelationSpoutSpark2Function(int numOfRouter, Config config) {
        this.numOfRouterBolts = numOfRouter;
        this.config = config;
    }

    @Override
    public Iterator<Tuple2<Integer, PartitionedEvent>> call(String message) {

        if (message == null) {
            return  Collections.emptyIterator();
        }

        SpecMetadataServiceClientImpl client = new SpecMetadataServiceClientImpl(config);
        sds = client.getSds();
        spoutSpec = client.getSpoutSpec();
        String[] topicAndContent = message.split(" ");
        if (topicAndContent.length < 2) {
            return  Collections.emptyIterator();
        }
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        Map<String, Object> value;
        try {
            value = mapper.readValue(topicAndContent[1], typeRef);
        } catch (IOException e) {
            LOG.error("covert tuple value to map error");
            return  Collections.emptyIterator();
        }
        List<Object> tuple = new ArrayList<Object>(2);
        String topic = topicAndContent[0];
        tuple.add(0, topic);
        tuple.add(1, value);
         /*Object topic = tupleContent.get(0);
        Object streamName = tupleContent.get(1);
        Object timestamp = tupleContent.get(2);
        Object value = tupleContent.get(3);*/
        Tuple2StreamMetadata metadata = spoutSpec.getTuple2StreamMetadataMap().get(topic);
        if (metadata == null) {
            LOG.error(
                    "tuple2StreamMetadata is null spout collector for topic {} see monitored metadata invalid, is this data source removed! ", topic);
            return  Collections.emptyIterator();
        }
        Tuple2StreamConverter converter = new Tuple2StreamConverter(metadata);
        List<Object> tupleContent = converter.convert(tuple);

        List<StreamRepartitionMetadata> streamRepartitionMetadataList = spoutSpec.getStreamRepartitionMetadataMap().get(topic);
        if (streamRepartitionMetadataList == null) {
            LOG.error(
                    "streamRepartitionMetadataList is nullspout collector for topic {} see monitored metadata invalid, is this data source removed! ", topic);
            return  Collections.emptyIterator();
        }
        Map<String, Object> messageContent = (Map<String, Object>) tupleContent.get(3);
        Object streamId = tupleContent.get(1);

        StreamDefinition sd = sds.get(streamId);
        if (sd == null) {
            LOG.warn("StreamDefinition {} is not found within {}, ignore this message", streamId, sds);
            return  Collections.emptyIterator();
        }
        List<Tuple2<Integer, PartitionedEvent>> outputTuple2s = new ArrayList<Tuple2<Integer, PartitionedEvent>>(5);

        Long timestamp = (Long) tupleContent.get(2);
        StreamEvent event = convertToStreamEventByStreamDefinition(timestamp, messageContent, sds.get(streamId));

        for (StreamRepartitionMetadata md : streamRepartitionMetadataList) {
            // one stream may have multiple group-by strategies, each strategy is for a specific group-by
            for (StreamRepartitionStrategy groupingStrategy : md.groupingStrategies) {
                int hash = 0;
                if (groupingStrategy.getPartition().getType().equals(StreamPartition.Type.GROUPBY)) {
                    hash = getRoutingHashByGroupingStrategy(messageContent, groupingStrategy);
                } else if (groupingStrategy.getPartition().getType().equals(StreamPartition.Type.SHUFFLE)) {
                    hash = Math.abs((int) System.currentTimeMillis());
                }
                int mod = hash % groupingStrategy.numTotalParticipatingRouterBolts;
                // filter out message
                if (mod >= groupingStrategy.startSequence && mod < groupingStrategy.startSequence + numOfRouterBolts) {
                    PartitionedEvent pEvent = new PartitionedEvent(event, groupingStrategy.partition, hash);
                    LOG.info(mod+"--------"+pEvent);
                    outputTuple2s.add(new Tuple2<Integer, PartitionedEvent>(mod, pEvent));
                }
            }
        }
        if (CollectionUtils.isEmpty(outputTuple2s)) {
            return  Collections.emptyIterator();
        }
        return outputTuple2s.iterator();
    }


    @Override
    public StreamDefinition getStreamDefinition(String streamId) {
        return sds.get(streamId);
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    private StreamEvent convertToStreamEventByStreamDefinition(long timestamp, Map messageContent, StreamDefinition sd) {
        return StreamEvent.Builder().timestamep(timestamp).attributes(messageContent, sd).build();
    }

}
