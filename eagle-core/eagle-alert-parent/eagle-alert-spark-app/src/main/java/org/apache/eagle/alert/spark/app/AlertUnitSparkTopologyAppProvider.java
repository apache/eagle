package org.apache.eagle.alert.spark.app;

import org.apache.eagle.app.spi.AbstractApplicationProvider;

public class AlertUnitSparkTopologyAppProvider extends AbstractApplicationProvider<AlertUnitSparkTopologyApp> {
    @Override
    public AlertUnitSparkTopologyApp getApplication() {
        return new AlertUnitSparkTopologyApp();
    }
}
