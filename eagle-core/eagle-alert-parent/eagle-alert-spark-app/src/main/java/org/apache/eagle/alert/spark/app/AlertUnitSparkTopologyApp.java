package org.apache.eagle.alert.spark.app;

import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.runner.UnitSparkUnionTopologyRunner;
import org.apache.eagle.app.SparkApplication;
import org.apache.eagle.app.environment.impl.SparkEnvironment;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class AlertUnitSparkTopologyApp extends SparkApplication {
    @Override
    public JavaStreamingContext execute(Config config, SparkEnvironment environment) {
        return new UnitSparkUnionTopologyRunner(config).buildTopology();
    }
}
