package org.apache.eagle.alert.engine.router;

import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;

import java.util.Map;

public interface SpecListener {
    void onSpecChange(SpoutSpec spec, RouterSpec routepec, Map<String, StreamDefinition> sds);
}
