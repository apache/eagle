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
package org.apache.eagle.log.entity.meta;

import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Qualifier implements Writable{
	private final static Logger LOG = LoggerFactory.getLogger(Qualifier.class);

	private String displayName;
	private String qualifierName;
	private EntitySerDeser<Object> serDeser;
	@JsonIgnore
	public EntitySerDeser<Object> getSerDeser() {
		return serDeser;
	}
	public void setSerDeser(EntitySerDeser<Object> serDeser) {
		this.serDeser = serDeser;
	}
	public String getDisplayName() {
		return displayName;
	}
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
	public String getQualifierName() {
		return qualifierName;
	}
	public void setQualifierName(String qualifierName) {
		this.qualifierName = qualifierName;
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("displayName:");
		sb.append(displayName);
		sb.append(",");
		sb.append("qualifierName:");
		sb.append(qualifierName);
		sb.append(",");
		sb.append("serDeser class:");
		sb.append(serDeser.getClass().getName());
		return sb.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(displayName);
		out.writeUTF(qualifierName);
		out.writeUTF(serDeser.getClass().getName());
	}

	private final static Map<String, EntitySerDeser> _entitySerDeserCache = new HashMap<String,EntitySerDeser>();

	@Override
	public void readFields(DataInput in) throws IOException {
		displayName = in.readUTF();
		qualifierName = in.readUTF();
		String serDeserClassName = in.readUTF();

		EntitySerDeser _cached = _entitySerDeserCache.get(serDeserClassName);
		if(_cached != null){
			this.serDeser = _cached;
		}else {
			try {
				if (LOG.isDebugEnabled()) LOG.debug("Creating new instance for " + serDeserClassName);
				Class serDeserClass = Class.forName(serDeserClassName);
				this.serDeser = (EntitySerDeser) serDeserClass.newInstance();
				_entitySerDeserCache.put(serDeserClassName, this.serDeser);
			} catch (Exception e) {
				if (LOG.isDebugEnabled()) {
					LOG.warn("Class not found for " + serDeserClassName + ": " + e.getMessage(), e);
				}
			}
		}
	}
}
