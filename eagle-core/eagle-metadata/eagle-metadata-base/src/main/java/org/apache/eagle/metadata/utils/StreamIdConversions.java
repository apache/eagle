/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metadata.utils;

import com.google.common.base.Preconditions;

/**
 * The name conversion about streamId:
 * 1. Case insensitive.
 * 2. All in upper case.
 */
public class StreamIdConversions {

    public static String formStreamTypeId(String streamTypeId) {
        return streamTypeId.toUpperCase();
    }

    public static String formatSiteStreamId(String siteId, String streamTypeId) {
        return String.format("%s_%s", streamTypeId, siteId).toUpperCase();
    }

    public static String parseStreamTypeId(String siteId, String generatedUniqueStreamId) {
        String subffix = String.format("_%s", siteId).toUpperCase();
        if (generatedUniqueStreamId.endsWith(subffix)) {
            int streamTypeIdLength = generatedUniqueStreamId.length() - subffix.length();
            Preconditions.checkArgument(streamTypeIdLength > 0, "Invalid streamId: " + generatedUniqueStreamId + ", streamTypeId is empty");
            return generatedUniqueStreamId.substring(0, streamTypeIdLength).toUpperCase();
        } else {
            throw new IllegalArgumentException("Invalid streamId: " + generatedUniqueStreamId + ", not end with \"" + subffix + "\"");
        }
    }
}