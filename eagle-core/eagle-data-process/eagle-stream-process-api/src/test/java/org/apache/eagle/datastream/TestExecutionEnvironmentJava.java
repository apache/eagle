package org.apache.eagle.datastream;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import junit.framework.Assert;
import org.apache.eagle.datastream.storm.StormExecutionEnvironment;
import org.junit.Test;

/**
 * @since 12/5/15
 */
public class TestExecutionEnvironmentJava {

    @Test
    public void testGetEnvInJava() {
        StormExecutionEnvironment env0 = ExecutionEnvironments.get(StormExecutionEnvironment.class);
        Assert.assertNotNull(env0);

        StormExecutionEnvironment env1 = ExecutionEnvironments.get(new String[]{}, StormExecutionEnvironment.class);
        Assert.assertNotNull(env1);
        Config config = ConfigFactory.load();
        StormExecutionEnvironment env2 = ExecutionEnvironments.get(config, StormExecutionEnvironment.class);
        Assert.assertNotNull(env2);
    }
}