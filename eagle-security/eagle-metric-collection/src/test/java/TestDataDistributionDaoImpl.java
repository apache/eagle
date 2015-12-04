/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.partition.DataDistributionDao;
import org.apache.eagle.security.partition.DataDistributionDaoImpl;

public class TestDataDistributionDaoImpl {

    //@Test
    public void test() throws Exception{
        System.setProperty("config.resource", "/application.local.conf");
        Config config = ConfigFactory.load();
        String eagleServiceHost = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
        Integer eagleServicePort = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);
        String username = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME);
        String password = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD);
        String topic = config.getString("dataSourceConfig.topic");
        DataDistributionDao dao = new DataDistributionDaoImpl(eagleServiceHost, eagleServicePort, username, password, topic);
        dao.fetchDataDistribution(System.currentTimeMillis() - 2 * DateUtils.MILLIS_PER_DAY, System.currentTimeMillis());
    }
}
