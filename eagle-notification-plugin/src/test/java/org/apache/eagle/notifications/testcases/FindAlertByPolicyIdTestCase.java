package org.apache.eagle.notifications.testcases;

import com.typesafe.config.Config;
import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.dao.AlertDefinitionDAOImpl;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.junit.Test;

public class FindAlertByPolicyIdTestCase {

	@Test
	public void testFindAlertDefByPolicyId(){
		Config config = EagleConfigFactory.load().getConfig();
		AlertDefinitionDAO dao = new AlertDefinitionDAOImpl(new EagleServiceConnector(config));

	}
}
