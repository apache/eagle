package org.apache.eagle.app.environment.impl;

import com.typesafe.config.Config;
import org.apache.eagle.app.environment.AbstractEnvironment;

/**
 * Beam Execution Environment Context.
 */
public class BeamEnviroment extends AbstractEnvironment {
    public BeamEnviroment(Config config) {
        super(config);
    }
}
