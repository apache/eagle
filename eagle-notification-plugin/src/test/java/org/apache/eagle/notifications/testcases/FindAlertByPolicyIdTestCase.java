package org.apache.eagle.notifications.testcases;

import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.dao.AlertDefinitionDAOImpl;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.junit.Test;

public class FindAlertByPolicyIdTestCase {

	@Test
	public void testFindAlertDefByPolicyId(){
	
		AlertDefinitionDAO dao = new AlertDefinitionDAOImpl(new EagleServiceConnector(null, null));
	}
}
