package org.apache.eagle.alert.coordination.model;

import java.util.Map;

/**
 * This metadata controls how to figure out stream name from incoming tuple
 */
public interface StreamNameSelector {
    /**
     * field name to value mapping
     * @param tuple
     * @return
     */
    String getStreamName(Map<String, Object> tuple);
}
