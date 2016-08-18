package org.apache.eagle.alert.engine.utils;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.PolicyHandlerContext;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;

import java.util.Map;

/**
 * Created on 8/16/16.
 */
public class AlertStreamUtils {

    /**
     * Create alert stream event for publisher.
     */
    public static AlertStreamEvent createAlertEvent(StreamEvent event,
                                                    PolicyHandlerContext context,
                                                    Map<String, StreamDefinition> sds) {
        PolicyDefinition policyDef = context.getPolicyDefinition();
        AlertStreamEvent alertStreamEvent = new AlertStreamEvent();

        alertStreamEvent.setTimestamp(event.getTimestamp());
        alertStreamEvent.setData(event.getData());
        alertStreamEvent.setStreamId(policyDef.getOutputStreams().get(0));
        alertStreamEvent.setPolicy(policyDef);

        if (context.getPolicyEvaluator() != null) {
            alertStreamEvent.setCreatedBy(context.getPolicyEvaluator().getName());
        }

        alertStreamEvent.setCreatedTime(System.currentTimeMillis());

        String is = policyDef.getInputStreams().get(0);
        StreamDefinition sd = sds.get(is);
        alertStreamEvent.setSchema(sd);

        return alertStreamEvent;
    }
}
