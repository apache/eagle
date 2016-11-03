package org.apache.eagle.security.oozie.parse;

import org.apache.eagle.app.spi.AbstractApplicationProvider;

/**
 * Since 10/25/16.
 */
public class OozieAuditLogAppProvider extends AbstractApplicationProvider<OozieAuditLogApplication> {

    public OozieAuditLogApplication getApplication() {
        return new OozieAuditLogApplication();
    }
}
