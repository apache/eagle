package org.apache.eagle.app;

import org.apache.eagle.app.environment.impl.SparkEnvironment;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public abstract class SparkApplication extends ExecutableApplication<SparkEnvironment, JavaStreamingContext> {
    @Override
    public Class<? extends SparkEnvironment> getEnvironmentType() {
        return SparkEnvironment.class;
    }
}
