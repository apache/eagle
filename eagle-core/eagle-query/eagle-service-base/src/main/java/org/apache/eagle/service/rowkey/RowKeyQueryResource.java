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
/**
 * 
 */
package org.apache.eagle.service.rowkey;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.eagle.service.common.EagleExceptionWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.base.taggedlog.NoSuchRowException;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.HBaseInternalLogHelper;
import org.apache.eagle.log.entity.InternalLog;
import org.apache.eagle.log.entity.RowkeyQueryAPIResponseEntity;
import org.apache.eagle.log.entity.index.RowKeyLogReader;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.common.EagleBase64Wrapper;

/**
 * @since Jan 26, 2015
 */
@Path("/rowkeyquery")
public class RowKeyQueryResource {
	private static final Logger LOG = LoggerFactory.getLogger(RowKeyQueryResource.class);

	@GET
	@Produces({MediaType.APPLICATION_JSON})
	public RowkeyQueryAPIResponseEntity getEntityByRowkey(@QueryParam("query") String query, @QueryParam("rowkey") String rowkey){
		RowkeyQueryAPIResponseEntity result = new RowkeyQueryAPIResponseEntity();
		RowKeyLogReader reader = null;

		try {
			EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName(query);
			reader = new RowKeyLogReader(ed, EagleBase64Wrapper.decode(rowkey));
			reader.open();
			InternalLog log = reader.read();
			TaggedLogAPIEntity entity;
			entity = HBaseInternalLogHelper.buildEntity(log, ed);
			result.setObj(entity);			
			result.setSuccess(true);
			return result;
		}
		catch(NoSuchRowException ex){
			LOG.error("rowkey " + ex.getMessage() + " does not exist!", ex);
			result.setSuccess(false);
			result.setException(EagleExceptionWrapper.wrap(ex));
			return result;
		}
		catch(Exception ex){
			LOG.error("Cannot read alert by rowkey", ex);
			result.setSuccess(false);
			result.setException(EagleExceptionWrapper.wrap(ex));
			return result;
		}
		finally{
			try {
				if(reader != null)
					reader.close();
			} catch (IOException e) {
			}
		}
	}
}
