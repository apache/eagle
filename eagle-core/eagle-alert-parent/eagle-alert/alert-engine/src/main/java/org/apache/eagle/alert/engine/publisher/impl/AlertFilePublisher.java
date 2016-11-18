/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *  <p/>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p/>
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.publisher.impl;

import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.PublishConstants;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;

public class AlertFilePublisher extends AbstractPublishPlugin {

    private Logger filelogger = Logger.getLogger(AlertFilePublisher.class.getName());;
    private FileHandler handler;


    private static final String DEFAULT_FILE_NAME = "eagle-alert.log";
    private static final int DEFAULT_ROTATE_SIZE_KB = 1024;
    private static final int DEFAULT_FILE_NUMBER = 5;

    @Override
    public void init(Config config, Publishment publishment, Map conf) throws Exception {
        super.init(config, publishment, conf);

        String fileName = DEFAULT_FILE_NAME;
        int rotateSize = DEFAULT_ROTATE_SIZE_KB;
        int numOfFiles = DEFAULT_FILE_NUMBER;
        if (publishment.getProperties() != null) {
            if (publishment.getProperties().containsKey(PublishConstants.FILE_NAME)) {
                fileName = (String) publishment.getProperties().get(PublishConstants.FILE_NAME);
            }
            if (publishment.getProperties().containsKey(PublishConstants.ROTATE_EVERY_KB)) {
                rotateSize = Integer.valueOf(publishment.getProperties().get(PublishConstants.ROTATE_EVERY_KB).toString());
            }
            if (publishment.getProperties().containsKey(PublishConstants.NUMBER_OF_FILES)) {
                numOfFiles = Integer.valueOf(publishment.getProperties().get(PublishConstants.NUMBER_OF_FILES).toString());
            }
        }
        handler = new FileHandler(fileName, rotateSize * 1024, numOfFiles, true);
        handler.setFormatter(new SimpleFormatter());
        filelogger.addHandler(handler);
        filelogger.setUseParentHandlers(false);
    }

    @Override
    public void onAlert(AlertStreamEvent event) throws Exception {
        List<AlertStreamEvent> eventList = this.dedup(event);
        if (eventList == null || eventList.isEmpty()) {
            return;
        }
        for (AlertStreamEvent e : eventList) {
            //filelogger.info(e.toString());
            filelogger.info(AlertPublishEvent.createAlertPublishEvent(e).toString());
        }
    }

    @Override
    public void close() {
        if (handler != null) {
            handler.close();
        }
    }

    @Override
    protected org.slf4j.Logger getLogger() {
        return LoggerFactory.getLogger(AlertFilePublisher.class);
    }
}
