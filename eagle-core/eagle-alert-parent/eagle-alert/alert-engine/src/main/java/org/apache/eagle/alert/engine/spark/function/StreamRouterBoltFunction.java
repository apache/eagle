package org.apache.eagle.alert.engine.spark.function;

import com.typesafe.config.Config;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.engine.StreamContext;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.router.StreamSortSpecListener;
import org.apache.eagle.alert.engine.router.impl.StreamRouterBoltOutputCollector;
import org.apache.eagle.alert.engine.serialization.PartitionedEventSerializer;
import org.apache.eagle.alert.engine.serialization.SerializationMetadataProvider;
import org.apache.eagle.alert.engine.serialization.Serializers;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;


public class StreamRouterBoltFunction   implements PairFlatMapFunction<Iterator<Tuple2<Integer, Object>>, Integer, Object>, SerializationMetadataProvider ,StreamSortSpecListener{

    private static final Logger LOG = LoggerFactory.getLogger(StreamRouterBoltFunction.class);
   // private StreamRouter router;
    private StreamRouterBoltOutputCollector routeCollector;
    // mapping from StreamPartition to StreamSortSpec
    private Map<StreamPartition, StreamSortSpec> cachedSSS = new HashMap<>();
    // mapping from StreamPartition(streamId, groupbyspec) to StreamRouterSpec
    private Map<StreamPartition, StreamRouterSpec> cachedSRS = new HashMap<>();

    private StreamContext streamContext;




    private Config config;
    private Map<String, StreamDefinition> sds;
    private RouterSpec routerSpec;
    private PartitionedEventSerializer serializer;
    public StreamRouterBoltFunction(Config config,RouterSpec routerSpec,Map<String, StreamDefinition> sds){
        this.config=config;
        this.routerSpec = routerSpec;
        this.serializer= Serializers.newPartitionedEventSerializer(this);

    }
    @Override
    public Iterable<Tuple2<Integer, Object>> call(Iterator<Tuple2<Integer, Object>> tuple2Iterator) throws Exception {

        List<Tuple2<Integer, Object>> result = new ArrayList<>();
        while (tuple2Iterator.hasNext()) {
            Tuple2<Integer, Object> a = tuple2Iterator.next();
            LOG.info(a._1() + "****" + a._2());
            PartitionedEvent pEvent = (PartitionedEvent) a._2;
            result.add(a);
        }
        LOG.info("******************");
        return result;
    }

    @Override
    public StreamDefinition getStreamDefinition(String streamId) {
        return null;
    }

    @Override
    public void onStreamSortSpecChange(Map<StreamPartition, StreamSortSpec> added, Map<StreamPartition, StreamSortSpec> removed, Map<StreamPartition, StreamSortSpec> changed) {
        //
    }
}
