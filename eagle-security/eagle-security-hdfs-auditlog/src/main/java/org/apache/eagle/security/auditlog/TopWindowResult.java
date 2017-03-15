/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.security.auditlog;

import java.io.Serializable;
import java.util.List;

public class TopWindowResult implements Serializable {
    private String timestamp;
    private List<TopWindow> windows;

    public List<TopWindow> getWindows() {
        return windows;
    }

    public void setWindows(List<TopWindow> windows) {
        this.windows = windows;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public static class TopWindow implements Serializable {
        private int windowLenMs;
        private List<Op> ops;

        public int getWindowLenMs() {
            return windowLenMs;
        }

        public void setWindowLenMs(int windowLenMs) {
            this.windowLenMs = windowLenMs;
        }

        public List<Op> getOps() {
            return ops;
        }

        public void setOps(List<Op> ops) {
            this.ops = ops;
        }

    }

    /**
     * Represents an operation within a TopWindow. It contains a ranked
     * set of the top users for the operation.
     */
    public static class Op implements Serializable {
        private String opType;
        private List<User> topUsers;
        private long totalCount;

        public String getOpType() {
            return opType;
        }

        public void setOpType(String opType) {
            this.opType = opType;
        }

        public List<User> getTopUsers() {
            return topUsers;
        }

        public void setTopUsers(List<User> topUsers) {
            this.topUsers = topUsers;
        }

        public long getTotalCount() {
            return totalCount;
        }

        public void setTotalCount(long totalCount) {
            this.totalCount = totalCount;
        }
    }

    /**
     * Represents a user who called an Op within a TopWindow. Specifies the
     * user and the number of times the user called the operation.
     */
    public static class User implements Serializable {
        private String user;
        private long count;

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

    }

}

