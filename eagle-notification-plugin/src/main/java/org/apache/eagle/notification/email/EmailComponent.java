package org.apache.eagle.notification.email;

import org.apache.eagle.common.metric.AlertContext;

/**
 * Email Component Obj
 */
public class EmailComponent {

    private AlertContext alertContext;
    public AlertContext getAlertContext() {
        return alertContext;
    }
    public void setAlertContext(AlertContext alertContext) {
        this.alertContext = alertContext;
    }
}
