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

package org.apache.eagle.jpm.util.resourcefetch.url;

import org.apache.eagle.jpm.util.Constants.JobState;
import org.junit.Assert;
import org.junit.Test;

public class JobListServiceURLBuilderImplTest {
    @Test
    public void testBuild() {
        JobListServiceURLBuilderImpl jobListServiceURLBuilderImpl = new JobListServiceURLBuilderImpl();
        String finalUrl = jobListServiceURLBuilderImpl.build("http://www.xxx.com:8088/", JobState.RUNNING.toString());
        Assert.assertEquals("http://www.xxx.com:8088/ws/v1/cluster/apps?state=RUNNING&anonymous=true", finalUrl);
        finalUrl = jobListServiceURLBuilderImpl.build("http://www.xxx.com:8088/", JobState.FINISHED.toString());
        Assert.assertEquals("http://www.xxx.com:8088/ws/v1/cluster/apps?state=FINISHED&anonymous=true", finalUrl);
        finalUrl = jobListServiceURLBuilderImpl.build("http://www.xxx.com:8088/", JobState.ALL.toString());
        Assert.assertEquals("http://www.xxx.com:8088/ws/v1/cluster/apps&anonymous=true", finalUrl);
        finalUrl = jobListServiceURLBuilderImpl.build("http://www.xxx.com:8088/", null);
        Assert.assertEquals("http://www.xxx.com:8088/ws/v1/cluster/apps&anonymous=true", finalUrl);
    }

}
