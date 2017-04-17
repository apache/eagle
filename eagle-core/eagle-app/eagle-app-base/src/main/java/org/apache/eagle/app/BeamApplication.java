package org.apache.eagle.app;

import org.apache.beam.sdk.Pipeline;
import org.apache.eagle.app.environment.impl.BeamEnviroment;

public abstract class BeamApplication extends ExecutableApplication<BeamEnviroment, Pipeline> {

    @Override
    public Class<? extends BeamEnviroment> getEnvironmentType() {
        return BeamEnviroment.class;
    }
}
