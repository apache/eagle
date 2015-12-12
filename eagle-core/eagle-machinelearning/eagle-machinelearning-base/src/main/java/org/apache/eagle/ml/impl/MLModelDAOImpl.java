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
package org.apache.eagle.ml.impl;

import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.ml.MLConstants;
import org.apache.eagle.ml.MLModelDAO;
import org.apache.eagle.ml.model.MLModelAPIEntity;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MLModelDAOImpl implements MLModelDAO {
	private final Logger LOG = LoggerFactory.getLogger(MLModelDAOImpl.class);
	private final EagleServiceConnector connector;

	public MLModelDAOImpl(EagleServiceConnector connector){
		this.connector = connector;
	}

	@Override
	public List<MLModelAPIEntity> findMLModelByContext(String user, String algorithm) {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = MLConstants.ML_MODEL_SERVICE_NAME + "[@user=\"" + user + "\" AND @algorithm=\""
						+ algorithm + "\"]{*}";
			GenericServiceAPIResponseEntity<MLModelAPIEntity> response =  client.search().startTime(0)
																		                 .endTime(10 * DateUtils.MILLIS_PER_DAY)
																		                 .pageSize(Integer.MAX_VALUE)
																		                 .query(query)
																	                     .send();
            if(!response.isSuccess()) {
                LOG.error(String.format("Failed to get model for user: %s, algorithm: %s, due to: %s",user,algorithm,response.getException()));
            }

            client.close();
            return response.getObj();
		} catch (Exception ex) {
			LOG.info("Got an exception when query machinelearning model service ", ex);
			throw new IllegalStateException(ex);
		}
	}
}
