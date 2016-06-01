package org.apache.eagle.alert.config;

public interface ConfigChangeCallback {
    void onNewConfig(ConfigValue value);
}
