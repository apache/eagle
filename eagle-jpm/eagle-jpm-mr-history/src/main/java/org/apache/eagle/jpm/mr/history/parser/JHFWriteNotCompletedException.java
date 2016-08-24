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

/**
 * used to warn that one job history file has not yet completed writing to hdfs
 * This happens when feeder catches up and the history file has not been written into hdfs completely
 *
 * @author yonzhang
 */
public class JHFWriteNotCompletedException extends Exception {
    /**
     *
     */
    private static final long serialVersionUID = -3060175780718218490L;

    public JHFWriteNotCompletedException(String msg) {
        super(msg);
    }
}
