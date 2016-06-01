package org.apache.eagle.alert.config;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ConfigBusProducer extends ConfigBusBase {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ConfigBusProducer.class);

    public ConfigBusProducer(ZKConfig config){
        super(config);
    }

    /**
     * @param topic
     * @param config
     */
    public void send(String topic, ConfigValue config){
        // check if topic exists, create this topic if not existing
        String zkPath = zkRoot + "/" + topic;
        try {
            if (curator.checkExists().forPath(zkPath) == null) {
                curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(zkPath);
            }
            ObjectMapper mapper = new ObjectMapper();
            byte[] content = mapper.writeValueAsBytes(config);
            curator.setData().forPath(zkPath, content);
        }catch(Exception ex){
            LOG.error("error creating zkPath " + zkPath, ex);
            throw new RuntimeException(ex);
        }
    }
}
