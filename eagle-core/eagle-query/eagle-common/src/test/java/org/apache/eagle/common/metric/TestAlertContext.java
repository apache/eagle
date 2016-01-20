package org.apache.eagle.common.metric;

import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * since 1/20/16.
 */
public class TestAlertContext {
    @Test
    public void test(){
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        AlertContext context = new AlertContext();
        context.addAll(map);
        String json = context.toJsonString();
        System.out.println(json);
        Assert.assertEquals(map, AlertContext.fromJsonString(json).getProperties());
    }
}
