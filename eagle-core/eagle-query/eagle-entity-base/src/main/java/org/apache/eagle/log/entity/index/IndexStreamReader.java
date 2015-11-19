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
package org.apache.eagle.log.entity.index;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.*;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.IndexDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public abstract class IndexStreamReader  extends StreamReader {
	protected final IndexDefinition indexDef;
	protected final SearchCondition condition;
	protected final List<byte[]> indexRowkeys;
	protected LogReader<InternalLog> reader;
	protected long lastTimestamp = 0;
	protected long firstTimestamp = 0;
	
	protected static final Logger LOG = LoggerFactory.getLogger(IndexStreamReader.class);

	public IndexStreamReader(IndexDefinition indexDef, SearchCondition condition, List<byte[]> indexRowkeys) {
		this.indexDef = indexDef;
		this.condition = condition;
		this.indexRowkeys = indexRowkeys;
		this.reader = null;
	}

	@Override
	public long getLastTimestamp() {
		return lastTimestamp;
	}

	@Override
	public long getFirstTimestamp() {
		return this.firstTimestamp;
	}

	@Override
	public void readAsStream() throws Exception {
		if (reader == null) {
			reader = createIndexReader();
		}
		final EntityDefinition entityDef = indexDef.getEntityDefinition();
		try{
			reader.open();
			InternalLog log;
			int count = 0;
			while ((log = reader.read()) != null) {
				TaggedLogAPIEntity entity = HBaseInternalLogHelper.buildEntity(log, entityDef);
				entity.setSerializeAlias(condition.getOutputAlias());
				entity.setSerializeVerbose(condition.isOutputVerbose());

				if (lastTimestamp == 0 || lastTimestamp < entity.getTimestamp()) {
					lastTimestamp = entity.getTimestamp();
				}
				if(firstTimestamp == 0 || firstTimestamp > entity.getTimestamp()){
					firstTimestamp = entity.getTimestamp();
				}
				for(EntityCreationListener l : _listeners){
					l.entityCreated(entity);
				}
				if(++count == condition.getPageSize()) {
					break;
				}
			}
		}catch(IOException ioe){
			LOG.error("Fail reading log", ioe);
			throw ioe;
		}finally{
			reader.close();
		}		
	}

	protected abstract LogReader createIndexReader();
	
}
