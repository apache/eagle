package org.apache.eagle.notifications.testcases;

import com.typesafe.config.Config;
import org.apache.eagle.alert.entity.AlertNotificationEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.notification.dao.AlertNotificationDAO;
import org.apache.eagle.notification.dao.AlertNotificationDAOImpl;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class GetAllNotifications {

    @Test
    public void  getAllNotification() throws Exception {
        Config config = EagleConfigFactory.load().getConfig();
        AlertNotificationDAO dao = new AlertNotificationDAOImpl( new EagleServiceConnector(config));
        List<AlertNotificationEntity> list = dao.findAlertNotificationTypes();
        System.out.println(" Fetch all Notifications : "+list);
    }
}
