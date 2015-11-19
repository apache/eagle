/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.jobrunning.callback;

import com.google.common.base.Objects;
import org.apache.eagle.jobrunning.common.JobConstants;

public class RunningJobMessageId {
	public String jobID;
	public JobConstants.ResourceType type;
	// If type = JOB_RUNNING_INFO, timestamp = fetchedTime, otherwise timestamp is meaningless, set to null
	public Long timestamp;
	
	public RunningJobMessageId(String jobID, JobConstants.ResourceType type, Long timestamp) {
		this.jobID = jobID;
		this.type = type;
		this.timestamp = timestamp;
	}

	@Override
	public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final RunningJobMessageId other = (RunningJobMessageId) obj;
        return Objects.equal(this.jobID, other.jobID) 
        	  && Objects.equal(this.type, other.type)
        	  && Objects.equal(this.timestamp, other.timestamp);
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(jobID.hashCode(), type.hashCode(), timestamp.hashCode());
	}
	
	@Override
	public String toString() {
		return "jobID=" + jobID 
			 + ", type=" + type.name() 
			 + ", timestamp= " + timestamp;
	}
}
