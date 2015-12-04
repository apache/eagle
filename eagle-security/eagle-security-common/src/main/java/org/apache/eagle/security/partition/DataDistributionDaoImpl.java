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

package org.apache.eagle.security.partition;

import com.sun.jersey.api.client.WebResource;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.metric.MetricConstants;
import org.apache.eagle.partition.DataDistributionDao;
import org.apache.eagle.partition.Weight;
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.EagleServiceSingleEntityQueryRequest;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataDistributionDaoImpl implements DataDistributionDao {

    private final Logger LOG = LoggerFactory.getLogger(DataDistributionDaoImpl.class);

    private final String eagleServiceHost;
    private final Integer eagleServicePort;
    private String username;
    private String password;
    private String topic;

    public DataDistributionDaoImpl(String eagleServiceHost, Integer eagleServicePort, String username, String password, String topic) {
        this.eagleServiceHost = eagleServiceHost;
        this.eagleServicePort = eagleServicePort;
        this.username = username;
        this.password = password;
        this.topic = topic;
    }

    @Override
    public List<Weight> fetchDataDistribution(long startTime, long endTime) throws Exception {
        IEagleServiceClient client = new EagleServiceClientImpl(eagleServiceHost, eagleServicePort, username, password) {
            @Override
            public <T extends Object> GenericServiceAPIResponseEntity<T> search(EagleServiceSingleEntityQueryRequest request) throws EagleServiceClientException {
                String queryString = request.getQueryParameterString();
                StringBuilder sb = new StringBuilder();
                sb.append("/list");
                sb.append("?");
                sb.append(queryString);
                final String urlString = sb.toString();
                if (!this.silence) LOG.info("Going to query service: " + this.getBaseEndpoint() + urlString);
                WebResource r = getWebResource(urlString);

                return putAuthHeaderIfNeeded(r.accept(DEFAULT_MEDIA_TYPE))
                        .header(CONTENT_TYPE, DEFAULT_HTTP_HEADER_CONTENT_TYPE)
                        .get(GenericServiceAPIResponseEntity.class);
            }
        };
        try {
            String query = MetricConstants.GENERIC_METRIC_ENTITY_ENDPOINT + "[@topic=\"" + topic + "\"]<@user>{sum(value)}.{sum(value) desc}";
            GenericServiceAPIResponseEntity<Map> response = client.search()
                    .startTime(startTime)
                    .endTime(endTime)
                    .pageSize(Integer.MAX_VALUE)
                    .query(query)
                    .metricName("kafka.message.user.count")
                    .send();
            if (!response.isSuccess()) {
                LOG.error(response.getException());
            }
            List<Weight> userWeights = new ArrayList<>();
            for (Map keyValue : response.getObj()) {
                List<String> keyList = (List)(keyValue.get("key"));
                List<Double> valueList = (List)(keyValue.get("value"));
                userWeights.add(new Weight(keyList.get(0), valueList.get(0)));
            }
            return userWeights;
        }
        catch (Exception ex) {
            LOG.error("Got an exception, ex: ", ex);
            throw new RuntimeException(ex);
        }
        finally {
            client.close();
        }
    }
}
