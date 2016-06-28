package org.apache.eagle.alert.engine.spark.function;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.coordination.model.StreamRepartitionMetadata;
import org.apache.eagle.alert.coordination.model.Tuple2StreamConverter;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.serialization.PartitionedEventSerializer;
import org.apache.eagle.alert.engine.serialization.SerializationMetadataProvider;
import org.apache.eagle.alert.engine.serialization.Serializers;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CorrelationSpoutSparkFunction implements Function<Tuple2<String, String>, Tuple2<Object, Object>>,SerializationMetadataProvider {

    private static final long serialVersionUID = -5281723341236671580L;
    private static final Logger LOG = LoggerFactory.getLogger(CorrelationSpoutSparkFunction.class);

    private String topologyId;
    private final Config config;

    private String topic = "";
    private PartitionedEventSerializer serializer;
    private SpoutSpec spoutSpec;
    private Map<String, StreamDefinition> sds;

    public CorrelationSpoutSparkFunction(String topic, String topologyId, Config config,SpoutSpec spoutSpec , Map<String, StreamDefinition> sds) {


        this.topologyId = topologyId;
        this.config = config;
        this.topic = topic;
        this.serializer = Serializers.newPartitionedEventSerializer(this);
        this.spoutSpec = spoutSpec;
        this.sds = sds;
      /*  kafkaSpoutMetric = new KafkaSpoutMetric();
        context.registerMetric("kafkaSpout", kafkaSpoutMetric, 60);*/
    }

    @Override
    public Tuple2<Object, Object> call(Tuple2<String, String> message) {

/**
 * Convert incoming tuple to stream
 * incoming tuple consists of 2 fields, topic and map of key/value
 * output stream consists of 3 fields, stream name, timestamp, and map of key/value
 */
        // cachedSpoutSpec get from broadcast var

        List<StreamRepartitionMetadata> streamRepartitionMetadataList = spoutSpec.getStreamRepartitionMetadataMap().get(topic);
        Tuple2StreamConverter converter = new Tuple2StreamConverter(spoutSpec.getTuple2StreamMetadataMap().get(topic));

        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        Map<String, Object> value;
        try {
            value = mapper.readValue(message._2, typeRef);
        } catch (IOException e) {
            LOG.error("covert tuple value to map error");
            return null;
        }
        List<Object> tuple = new ArrayList<Object>(2);
        tuple.add(0, topic);
        tuple.add(1, value);
        List<Object> result = converter.convert(tuple);
        /*Object topic = result.get(0);
        Object streamName = result.get(1);
        Object timestamp = result.get(2);
        Object kv = result.get(3);*/
        //key
        return new Tuple2<>(topic, result);
    }



    @Override
    public StreamDefinition getStreamDefinition(String streamId) {
        return sds.get(streamId);
    }

    /**
     * How to assert that numTotalGroupbyBolts >= numOfRouterBolts, otherwise
     * there is runtime issue by default, tuple includes 2 fields field 1: topic
     * name field 2: map of key/value
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
         if this topic multiplexes multiple streams, then retrieve the individual streams
         */
        List<Object> convertedTuple = converter.convert(tuple);
        if(convertedTuple == null) {
            LOG.warn("source data {} can't be converted to a stream, ignore this message", tuple);
            spout.ack(newMessageId);
            return null;
        }
        Map m = (Map)convertedTuple.get(3);
        Object streamId = convertedTuple.get(1);

        StreamDefinition sd = sds.get(streamId);
        if(sd == null){
            LOG.warn("StreamDefinition {} is not found within {}, ignore this message", streamId, sds);
            spout.ack(newMessageId);
            return null;
        }

        StreamEvent event = convertToStreamEventByStreamDefinition((Long)convertedTuple.get(2), m, sds.get(streamId));
        /**
            phase 2: stream repartition
        **/
        for(StreamRepartitionMetadata md : streamRepartitionMetadataList) {
            // one stream may have multiple group-by strategies, each strategy is for a specific group-by
            for(StreamRepartitionStrategy groupingStrategy : md.groupingStrategies){
                int hash = 0;
                if(groupingStrategy.getPartition().getType().equals(StreamPartition.Type.GROUPBY)) {
                    hash = getRoutingHashByGroupingStrategy(m, groupingStrategy);
                }else if(groupingStrategy.getPartition().getType().equals(StreamPartition.Type.SHUFFLE)){
                    hash = Math.abs((int)System.currentTimeMillis());
                }
                int mod = hash % groupingStrategy.numTotalParticipatingRouterBolts;
                // filter out message
                if (mod >= groupingStrategy.startSequence && mod < groupingStrategy.startSequence + numOfRouterBolts) {
                    // framework takes care of field grouping instead of using storm internal field grouping
                    String sid = StreamIdConversion.generateStreamIdBetween(spout.getSpoutName(), spout.getRouteBoltName()+ (hash % numOfRouterBolts));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Emitted tuple: {} with message Id: {}, with topic {}, to streamId {}", convertedTuple, messageId, topic, sid);
                    }
                    // send message to StreamRouterBolt
                    PartitionedEvent pEvent = new PartitionedEvent(event, groupingStrategy.partition, hash);
                    if(this.serializer == null){
                        delegate.emit(sid, Collections.singletonList(pEvent), newMessageId);
                    }else {
                        try {
                            delegate.emit(sid, Collections.singletonList(serializer.serialize(pEvent)), newMessageId);
                        } catch (IOException e) {
                            LOG.error("Failed to serialize {}", pEvent, e);
                            throw new RuntimeException(e);
                        }
                    }
                }else{
                    // ******* short-cut ack ********
                    // we should simply ack those messages which are not processed in this topology because KafkaSpout implementation requires _pending is empty
                    // before moving to next offsets.
                    if(LOG.isDebugEnabled()){
                        LOG.debug("Message filtered with mod {} not within range {} and {} for message {}", mod, groupingStrategy.startSequence,
                                groupingStrategy.startSequence+ numOfRouterBolts, tuple);
                    }
                    spout.ack(newMessageId);
                }
            }
        }

        return null;
    }

    @SuppressWarnings("rawtypes")
    private int getRoutingHashByGroupingStrategy(Map data, StreamRepartitionStrategy gs){
        // calculate hash value for values from group-by fields
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder();
        for(String groupingField : gs.partition.getColumns()) {
            if(data.get(groupingField) != null){
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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private StreamEvent convertToStreamEventByStreamDefinition(long timestamp, Map m, StreamDefinition sd){
        return StreamEvent.Builder().timestamep(timestamp).attributes(m,sd).build();
    }



}
