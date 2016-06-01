package org.apache.eagle.alert.config;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 1. When consumer is started, it always get notified of config
 * 2. When config is changed, consumer always get notified of config change
 *
 * Reliability issue:
 * TODO How to ensure config change message is always delivered to consumer
 */
public class ConfigBusConsumer extends ConfigBusBase {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ConfigBusConsumer.class);

    private NodeCache cache;
    public ConfigBusConsumer(ZKConfig config, String topic, ConfigChangeCallback callback){
        super(config);
        String zkPath = zkRoot + "/" + topic;
        LOG.info("monitor change for zkPath " + zkPath);
        cache = new NodeCache(curator, zkPath);
        cache.getListenable().addListener( () ->
            {
                // get node value and notify callback
                byte[] value = curator.getData().forPath(zkPath);
                ObjectMapper mapper = new ObjectMapper();
                ConfigValue v = mapper.readValue(value, ConfigValue.class);
                callback.onNewConfig(v);
            }
        );
        try {
            cache.start();
        }catch(Exception ex){
            LOG.error("error start NodeCache listener", ex);
            throw new RuntimeException(ex);
        }
    }
}
