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

package org.apache.eagle.notifications.testcases;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.metric.AlertContext;
import org.apache.eagle.notification.utils.NotificationPluginUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eagle on 29/1/16.
 */
public class NotificationConfTest {

    /*public static void main(String[] args ) throws Exception {
        List<AlertDefinitionAPIEntity> list = NotificationPluginUtils.fetchActiveAlerts();
        for(AlertDefinitionAPIEntity entity : list )
            try {
                AlertAPIEntity e = new AlertAPIEntity();
                Map<String, String> tags = new HashMap<>();
                tags.put(" type","email");
                e.setTags( tags );
                AlertContext context = new AlertContext();
                context.addAll(tags);
                ObjectMapper mapper = new ObjectMapper();
                String tmp = mapper.writeValueAsString(e);
                System.out.println(tmp);
                *//*CollectionType mapCollectionType = mapper.getTypeFactory().constructCollectionType(List.class, Map.class);
                List<Map<String, String>> map = mapper.readValue(entity.getNotificationDef(), mapCollectionType);
                System.out.println(map);*//*
            } catch (Exception ex) {
                ex.printStackTrace();
            }
    }*/
}
