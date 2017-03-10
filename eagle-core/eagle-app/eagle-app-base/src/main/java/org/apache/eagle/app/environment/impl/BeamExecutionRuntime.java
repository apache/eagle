package org.apache.eagle.app.environment.impl;

import com.typesafe.config.Config;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.environment.ExecutionRuntime;
import org.apache.eagle.app.environment.ExecutionRuntimeProvider;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BeamExecutionRuntime implements ExecutionRuntime<BeamEnviroment, Pipeline> {

    private static final Logger LOG = LoggerFactory.getLogger(BeamExecutionRuntime.class);

    private BeamEnviroment environment;
    private BeamRuntimeResultManager beamRuntimeResultManager;

    @Override
    public void prepare(BeamEnviroment environment) {
        this.environment = environment;
        this.beamRuntimeResultManager = BeamRuntimeResultManager.getInstance();
    }

    @Override
    public BeamEnviroment environment() {
        return this.environment;
    }

    @Override
    public void start(Application<BeamEnviroment, Pipeline> executor, Config config) {
        String appId = config.getString("appId");
        if (beamRuntimeResultManager.isAppRunning(appId)) {
            return;
        }
        Pipeline pipeline = executor.execute(config, environment);
        // Run the pipeline.
        SparkPipelineResult res = (SparkPipelineResult) pipeline.run();
        beamRuntimeResultManager.insertResult(appId, res);
        res.waitUntilFinish();
    }

    @Override
    public void stop(Application<BeamEnviroment, Pipeline> executor, Config config) {
        SparkPipelineResult res = beamRuntimeResultManager.getResult(config.getString("appId"));
        if (res != null) {
            try {
                res.cancel();
            } catch (IOException ex) {
                LOG.error("Got an exception when stop, ex: ", ex);
            }
        }

    }

    @Override
    public ApplicationEntity.Status status(Application<BeamEnviroment, Pipeline> executor, Config config) {
        SparkPipelineResult res = beamRuntimeResultManager.getResult(config.getString("appId"));
        ApplicationEntity.Status status;
        if (res == null) {
            LOG.error("Unknown storm topology  status res is null");
            status = ApplicationEntity.Status.UNKNOWN;
            return status;
        }
        PipelineResult.State state = res.getState();

        if (state == PipelineResult.State.RUNNING) {
            status = ApplicationEntity.Status.RUNNING;
        } else if (state == PipelineResult.State.FAILED || state == PipelineResult.State.STOPPED || state == PipelineResult.State.CANCELLED) {
            return ApplicationEntity.Status.STOPPED;
        } else {
            LOG.error("Unknown storm topology  status");
            status = ApplicationEntity.Status.UNKNOWN;
        }
        return status;
    }

    public static class Provider implements ExecutionRuntimeProvider<BeamEnviroment, Pipeline> {
        @Override
        public ExecutionRuntime<BeamEnviroment, Pipeline> get() {
            return new BeamExecutionRuntime();
        }
    }
}
