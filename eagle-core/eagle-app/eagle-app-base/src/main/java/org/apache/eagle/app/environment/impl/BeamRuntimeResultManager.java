package org.apache.eagle.app.environment.impl;

import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.sdk.PipelineResult;

import java.util.HashMap;
import java.util.Map;

public class BeamRuntimeResultManager {
    private static final BeamRuntimeResultManager INSTANCE = new BeamRuntimeResultManager();
    private final Map<String, SparkPipelineResult> sparkPipelineResultCache;

    public static BeamRuntimeResultManager getInstance() {
        return INSTANCE;
    }

    private BeamRuntimeResultManager() {
        sparkPipelineResultCache = new HashMap<>();
    }

    synchronized boolean isAppRunning(String appId) {
        return sparkPipelineResultCache.containsKey(appId) && sparkPipelineResultCache.get(appId).getState() == PipelineResult.State.RUNNING;
    }

    synchronized void insertResult(String appId, SparkPipelineResult rs) {

        sparkPipelineResultCache.put(appId, rs);
    }

    synchronized SparkPipelineResult getResult(String appId) {
        return sparkPipelineResultCache.get(appId);
    }

    public synchronized void removeResult(String appId) {
        sparkPipelineResultCache.remove(appId);
    }
}
