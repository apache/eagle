package org.apache.eagle.jpm.mr.running.entities;

import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("eagleMRRunningTasks")
@ColumnFamily("f")
@Prefix("tasks_exec_attempt")
@Service(Constants.JPA_RUNNING_TASK_ATTEMPT_EXECUTION_SERVICE_NAME)
@TimeSeries(true)
@Partition({"site"})

@Tags({"site", "jobId", "JobName", "jobNormalName", "jobType", "taskType", "taskId", "user", "queue", "host", "rack"})
public class TaskAttemptExecutionAPIEntity extends TaggedLogAPIEntity {
    @Column("a")
    private long startTime;
    @Column("b")
    private long finishTime;
    @Column("c")
    private long elapsedTime;
    @Column("d")
    private double progress;
    @Column("e")
    private String id;
    @Column("f")
    private String status;
    @Column("g")
    private String diagnostics;
    @Column("h")
    private String type;
    @Column("i")
    private String assignedContainerId;

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
        valueChanged("startTime");
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
        valueChanged("finishTime");
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
        valueChanged("elapsedTime");
    }

    public double getProgress() {
        return progress;
    }

    public void setProgress(double progress) {
        this.progress = progress;
        valueChanged("progress");
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
        valueChanged("id");
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        valueChanged("status");
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
        valueChanged("diagnostics");
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
        valueChanged("type");
    }

    public String getAssignedContainerId() {
        return assignedContainerId;
    }

    public void setAssignedContainerId(String assignedContainerId) {
        this.assignedContainerId = assignedContainerId;
        valueChanged("assignedContainerId");
    }
}
