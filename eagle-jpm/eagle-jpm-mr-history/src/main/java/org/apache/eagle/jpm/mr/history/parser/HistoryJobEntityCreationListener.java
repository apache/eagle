/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.mr.history.parser;


import org.apache.eagle.jpm.mr.historyentity.JobBaseAPIEntity;

/**
 * generalizing this listener would decouple entity creation and entity handling, also will help unit testing.
 */
public interface HistoryJobEntityCreationListener {
    /**
     * job entity created event.
     *
     * @param entity
     */
    void jobEntityCreated(JobBaseAPIEntity entity) throws Exception;

    /**
     * for streaming processing, flush would help commit the last several entities.
     */
    void flush() throws Exception;
}
