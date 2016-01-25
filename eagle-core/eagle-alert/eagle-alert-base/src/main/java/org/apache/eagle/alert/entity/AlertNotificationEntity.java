package org.apache.eagle.alert.entity;

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;


@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("alertNotifications")
@ColumnFamily("f")
@Prefix("alertNotifications")
@Service(AlertConstants.ALERT_NOTIFICATION_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"notificationType"})
public class AlertNotificationEntity extends TaggedLogAPIEntity {
    @Column("a")
    private boolean enabled;
    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        valueChanged("enabled");
    }
}
