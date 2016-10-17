/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.coordinator;

import java.io.IOException;

public class StreamNotDefinedException extends IOException {
    private static final long serialVersionUID = 6027811718016485808L;

    public StreamNotDefinedException() {
    }

    public StreamNotDefinedException(String streamId) {
        super("Stream definition not found: " + streamId);
    }

    public StreamNotDefinedException(String streamName, String specVersion) {
        super(String.format("Stream '%s' not found! Current spec version '%s'. Possibly metadata not loaded or metadata mismatch between upstream and alert bolts yet!", streamName, specVersion));
    }

    public StreamNotDefinedException(String streamName, String streamMetaVersion, String specVersion) {
        super(String.format("Stream '%s' has meta version '%s' which is different from current spec version '%s'.", streamName, streamMetaVersion, specVersion));
    }
}