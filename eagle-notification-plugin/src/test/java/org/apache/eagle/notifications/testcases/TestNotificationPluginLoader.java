package org.apache.eagle.notifications.testcases;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import junit.framework.Assert;
import org.apache.eagle.notification.plugin.NewNotificationPluginLoader;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created on 2/10/16.
 */
public class TestNotificationPluginLoader {
    @Ignore //only work when connected to eagle service
    @Test
    public void testLoader(){
        Config config = ConfigFactory.load();
        NewNotificationPluginLoader loader = NewNotificationPluginLoader.getInstance();
        loader.init(config);
        Assert.assertTrue(loader.getNotificationMapping().keySet().contains("Eagle Store"));
        Assert.assertTrue(loader.getNotificationMapping().keySet().contains("Kafka Store"));
        Assert.assertTrue(loader.getNotificationMapping().keySet().contains("Email Notification"));
    }
}
