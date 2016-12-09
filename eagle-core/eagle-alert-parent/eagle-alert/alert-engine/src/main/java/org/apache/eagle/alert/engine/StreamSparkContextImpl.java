package org.apache.eagle.alert.engine;

import com.typesafe.config.Config;

import java.io.Serializable;

public class StreamSparkContextImpl implements StreamContext, Serializable {

    private static final long serialVersionUID = -2769182688128199150L;
    private final Config config;
    private final StreamCounter counter;

    public StreamSparkContextImpl(Config config) {
        this.counter = new SparkCountMetric();
        this.config = config;
    }

    @Override
    public StreamCounter counter() {
        return counter;
    }

    @Override
    public Config config() {
        return config;
    }
}
