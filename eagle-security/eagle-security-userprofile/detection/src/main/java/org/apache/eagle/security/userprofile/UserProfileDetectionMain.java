/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.eagle.security.userprofile;

import com.typesafe.config.Config;
import org.apache.eagle.dataproc.util.ConfigOptionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UserProfileDetectionMain {
    private final static Logger LOG = LoggerFactory.getLogger(UserProfileDetectionMain.class);
    public final static String USERPROFILE_DETECTION_MODE_KEY="eagleProps.userProfileMode";
    public static void main(String[] args) throws Exception {
        Config config = new ConfigOptionParser().load(args);
        LOG.info("Config class: " + config.getClass().getCanonicalName());

        if(config.hasPath(USERPROFILE_DETECTION_MODE_KEY) && config.getString(USERPROFILE_DETECTION_MODE_KEY).equalsIgnoreCase("batch")){
            LOG.info("Starting UserProfileDetection Topology in [batch] mode");
            UserProfileDetectionBatchMain.main(args);
        }else{
            LOG.info("Starting UserProfileDetection Topology in [streaming] mode");
            UserProfileDetectionStreamMain.main(args);
        }
    }
}