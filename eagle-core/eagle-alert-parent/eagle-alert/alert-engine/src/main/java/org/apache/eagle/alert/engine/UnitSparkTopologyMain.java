package org.apache.eagle.alert.engine;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.config.ZKConfig;
import org.apache.eagle.alert.config.ZKConfigBuilder;
import org.apache.eagle.alert.engine.coordinator.impl.ZKMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.runner.UnitSparkTopologyRunner;

public class UnitSparkTopologyMain {
    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        ZKConfig zkConfig = ZKConfigBuilder.getZKConfig(config);
        String topologyId = config.getString("topology.name");
        ZKMetadataChangeNotifyService changeNotifyService = new ZKMetadataChangeNotifyService(zkConfig, topologyId);

        new UnitSparkTopologyRunner(changeNotifyService,config).run();
    }

  /*  public static StormTopology createTopology(Config config) {
        ZKConfig zkConfig = ZKConfigBuilder.getZKConfig(config);
        String topologyId = config.getString("topology.name");
        ZKMetadataChangeNotifyService changeNotifyService = new ZKMetadataChangeNotifyService(zkConfig, topologyId);

        return new UnitTopologyRunner(changeNotifyService).buildTopology(topologyId, config);
    }*/
}
