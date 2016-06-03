package org.apache.eagle.jpm.mr.history.entities;

import org.apache.eagle.jpm.mr.history.common.JPAConstants;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("eaglejpa_process")
@ColumnFamily("f")
@Prefix("process")
@Service(JPAConstants.JPA_JOB_PROCESS_TIME_STAMP_NAME)
@TimeSeries(true)
@Partition({"site"})
public class JobProcessTimeStampEntity extends TaggedLogAPIEntity {
    @Column("a")
    private long currentTimeStamp;

    public long getCurrentTimeStamp() {
        return currentTimeStamp;
    }
    public void setCurrentTimeStamp(long currentTimeStamp) {
        this.currentTimeStamp = currentTimeStamp;
        _pcs.firePropertyChange("currentTimeStamp", null, null);
    }
}
