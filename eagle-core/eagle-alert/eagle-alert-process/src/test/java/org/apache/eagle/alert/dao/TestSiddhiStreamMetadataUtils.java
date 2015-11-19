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
package org.apache.eagle.alert.dao;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import junit.framework.Assert;

import org.apache.eagle.alert.entity.AlertStreamSchemaEntity;
import org.apache.eagle.alert.siddhi.SiddhiStreamMetadataUtils;
import org.apache.eagle.alert.siddhi.StreamMetadataManager;
import org.junit.Test;

public class TestSiddhiStreamMetadataUtils {
	@Test
	public void test() throws Exception{
        Config config = ConfigFactory.load();
		StreamMetadataManager.getInstance().init(config, new AlertStreamSchemaDAO(){
			@Override
			public List<AlertStreamSchemaEntity> findAlertStreamSchemaByDataSource(
                    String dataSource) {
				return Arrays.asList(generateStreamMetadataAPIEntity("attrName1", "STRING"),
						generateStreamMetadataAPIEntity("attrName2", "LONG")
						);
			}
		});
		String siddhiStreamDef = SiddhiStreamMetadataUtils.convertToStreamDef("testStreamName");
		Assert.assertEquals("define stream " + "testStreamName" + "(eagleAlertContext object,attrName1 string,attrName2 long);", siddhiStreamDef);
		StreamMetadataManager.getInstance().reset();
	}
	
	private AlertStreamSchemaEntity generateStreamMetadataAPIEntity(final String attrName, String attrType){
		AlertStreamSchemaEntity entity = new AlertStreamSchemaEntity();
		entity.setTags(new HashMap<String, String>(){{
			put("programId", "testProgramId");
			put("streamName", "testStreamName");
			put("attrName", attrName);
		}});
		entity.setAttrType(attrType);
		return entity;
	}
}
