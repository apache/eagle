package org.apache.eagle.policy.dao;

import org.apache.eagle.alert.entity.AlertStreamSchemaEntity;
import org.junit.Test;

import java.util.List;

/**
 * Created on 12/31/15.
 */
public class TestSchemaDao {

    @Test
    public void test() throws Exception {
        AlertStreamSchemaDAO dao = new AlertStreamSchemaDAOImpl("eagle-c3-lvs01-1-9953.lvs01.dev.ebayc3.com", 9099, "admin", "secret");
        List<AlertStreamSchemaEntity> entities = dao.findAlertStreamSchemaByDataSource("hdfsAuditLog");
        System.out.print(entities);
    }
}
