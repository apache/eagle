package org.apache.eagle.app.environment.impl;

import com.typesafe.config.Config;
import org.apache.beam.sdk.Pipeline;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.environment.ExecutionRuntime;
import org.apache.eagle.app.environment.ExecutionRuntimeProvider;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamExecutionRuntime implements ExecutionRuntime<BeamEnviroment, Pipeline> {

    private static final Logger LOG = LoggerFactory.getLogger(BeamExecutionRuntime.class);

    private BeamEnviroment environment;

    @Override
    public void prepare(BeamEnviroment environment) {
        this.environment = environment;
    }

    @Override
    public BeamEnviroment environment() {
        return this.environment;
    }

    @Override
    public void start(Application<BeamEnviroment, Pipeline> executor, Config config) {
        Pipeline pipeline  = executor.execute(config, environment);
        pipeline.run();
    }

    @Override
    public void stop(Application<BeamEnviroment, Pipeline> executor, Config config) {

    }

    @Override
    public ApplicationEntity.Status status(Application<BeamEnviroment, Pipeline> executor, Config config) {
        return null;
    }

    public static class Provider implements ExecutionRuntimeProvider<BeamEnviroment, Pipeline> {
        @Override
        public ExecutionRuntime<BeamEnviroment, Pipeline> get() {
            return new BeamExecutionRuntime();
        }
    }
}
