package org.apache.eagle.alert.engine.spark.function2;


import com.typesafe.config.Config;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertPublisher;
import org.apache.eagle.alert.engine.publisher.impl.AlertPublisherImpl;
import org.apache.eagle.alert.engine.runner.MapComparator;
import org.apache.eagle.alert.service.SpecMetadataServiceClientImpl;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

public class AlertPublisherBoltSpark2Function implements ForeachPartitionFunction<Tuple2<String, AlertStreamEvent>> {

    private final static Logger LOG = LoggerFactory.getLogger(AlertPublisherBoltSpark2Function.class);

    private Map<String, Publishment> cachedPublishments = new HashMap<>();

    private PublishSpec pubSpec;
    private Map<String, StreamDefinition> sds;
    private String alertPublishBoltName = "alertPublishBolt";
    private Config config;


    public AlertPublisherBoltSpark2Function(Config config, String alertPublishBoltName) {
        this.alertPublishBoltName = alertPublishBoltName;
        this.config = config;
    }

    @Override
    public void call(Iterator<Tuple2<String, AlertStreamEvent>> tuple2Iterator) throws Exception {

        AlertPublisher alertPublisher = new AlertPublisherImpl(alertPublishBoltName);
        alertPublisher.init(null, new HashMap<>());
        SpecMetadataServiceClientImpl client = new SpecMetadataServiceClientImpl(config);
        pubSpec = client.getPublishSpec();
        sds = client.getSds();
        onAlertPublishSpecChange(alertPublisher, pubSpec, sds);
        while (tuple2Iterator.hasNext()) {
            Tuple2<String, AlertStreamEvent> tuple2 = tuple2Iterator.next();
            AlertStreamEvent alertEvent = tuple2._2;
            LOG.info("AlertPublisherBoltFunction " + alertEvent);
            alertPublisher.nextEvent((alertEvent));
        }

    }

    public void onAlertPublishSpecChange(AlertPublisher alertPublisher, PublishSpec pubSpec, Map<String, StreamDefinition> sds) {
        if (pubSpec == null) return;

        List<Publishment> newPublishments = pubSpec.getPublishments();
        if (newPublishments == null) {
            LOG.info("no publishments with PublishSpec {} for this topology", pubSpec);
            return;
        }

        Map<String, Publishment> newPublishmentsMap = new HashMap<>();
        newPublishments.forEach(p -> newPublishmentsMap.put(p.getName(), p));
        MapComparator<String, Publishment> comparator = new MapComparator<>(newPublishmentsMap, cachedPublishments);
        comparator.compare();

        List<Publishment> beforeModified = new ArrayList<>();
        comparator.getModified().forEach(p -> beforeModified.add(cachedPublishments.get(p.getName())));
        alertPublisher.onPublishChange(comparator.getAdded(), comparator.getRemoved(), comparator.getModified(), beforeModified);

        // switch
        cachedPublishments = newPublishmentsMap;
    }
}
