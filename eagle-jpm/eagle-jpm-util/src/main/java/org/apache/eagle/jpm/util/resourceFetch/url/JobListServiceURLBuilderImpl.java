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

import org.apache.eagle.jpm.util.Constants;

public class JobListServiceURLBuilderImpl implements ServiceURLBuilder {

    public String build(String... parameters) {
        /**
         * {rmUrl}/ws/v1/cluster/apps?state=RUNNING.
         * We need to remove tailing slashes to avoid "url//ws/v1"
         * because it would not be found and would be redirected to
         * history server ui.
         */
        String rmUrl = URLUtil.removeTrailingSlash(parameters[0]);

        String restApi = null;
        String jobState = parameters[1];

        if (jobState.equals(Constants.JobState.RUNNING.name())) {
            restApi = Constants.V2_APPS_RUNNING_URL;
        } else if (jobState.equals(Constants.JobState.FINISHED.name())) {
            restApi = Constants.V2_APPS_COMPLETED_URL;
        } else if (jobState.equals(Constants.JobState.ALL.name())) {
            restApi = Constants.V2_APPS_URL;
        }
        if (restApi == null) {
            return null;
        }
        // "/ws/v1/cluster/apps?state=RUNNING"
        StringBuilder sb = new StringBuilder();
        sb.append(rmUrl).append("/").append(restApi);
        sb.append("&").append(Constants.ANONYMOUS_PARAMETER);

        return sb.toString();
    }
}
