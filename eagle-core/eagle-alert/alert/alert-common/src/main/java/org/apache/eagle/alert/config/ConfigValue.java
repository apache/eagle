package org.apache.eagle.alert.config;


/**
 * Config body contains actual data for one topic
 * this is serialized with json format into zookeeper
 * value can be versionId which is used for referencing outside data
 * or value can be actual config value
 */
public class ConfigValue {
    private boolean isValueVersionId;
    private Object value;

    public boolean isValueVersionId() {
        return isValueVersionId;
    }

    public void setValueVersionId(boolean valueVersionId) {
        isValueVersionId = valueVersionId;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String toString(){
        return "isValueVersionId: " + isValueVersionId + ", value: " + value;
    }
}
