package org.apache.eagle.notification.utils;

import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;

import java.util.HashMap;
import java.util.Map;

public class NotificationPluginUtils {

	public static ConfigObject _conf;
	
	public static ConfigObject getNotificationConfigObj() throws Exception
	{
		Config config = EagleConfigFactory.load().getConfig();
		if( config.getObject("eagleNotificationProps") == null )
			throw new Exception("Eagle Notification Properties not found in application.conf ");
		return config.getObject("eagleNotificationProps");
	}
		
	public static String getPropValue(String key ) throws Exception {

		if (_conf == null)
			_conf = getNotificationConfigObj();
		return _conf.get(key).unwrapped().toString();
	}
}
