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
package org.apache.eagle.log.entity;

import org.apache.eagle.common.EagleExceptionWrapper;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.List;
import java.util.Map;

/**
 * The only GenericServiceAPIResponseEntity for both client and server side
 *
 * @see GenericServiceAPIResponseEntityDeserializer
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {"success","exception","meta","type","obj"})
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonDeserialize(using = GenericServiceAPIResponseEntityDeserializer.class)
@JsonIgnoreProperties(ignoreUnknown=true)
public class GenericServiceAPIResponseEntity<T>{
    /**
     * Please use primitive type of value in meta as possible
     */
    private Map<String,Object> meta;
	private boolean success;
	private String exception;
    private List<T> obj;
    private Class<T> type;

    public GenericServiceAPIResponseEntity(){
        // default constructor
    }
    public GenericServiceAPIResponseEntity(Class<T> type){
        this.setType(type);
    }

    public Map<String, Object> getMeta() {
        return meta;
    }

    public void setMeta(Map<String, Object> meta) {
        this.meta = meta;
    }

    public List<T> getObj() {
        return obj;
    }

    public void setObj(List<T> obj) {
        this.obj = obj;
    }

    public void setObj(List<T> obj,Class<T> type) {
        this.setObj(obj);
        this.setType(type);
    }

    public Class<T> getType() {
        return type;
    }

    /**
     * Set the first object's class as type
     */
    @SuppressWarnings("unused")
    public void setTypeByObj(){
        for(T t:this.obj){
            if(this.type == null && t!=null){
                this.type = (Class<T>) t.getClass();
            }
        }
    }

    /**
     * can explicitly change type class
     *
     * @param type
     */
    public void setType(Class<T> type) {
        this.type = type;
    }

	public boolean isSuccess() {
		return success;
	}
	public void setSuccess(boolean success) {
		this.success = success;
	}
	public String getException() {
		return exception;
	}
	public void setException(String exception) {
		this.exception = exception;
	}

    public void setException(Exception exception){
        if(exception!=null) this.exception = EagleExceptionWrapper.wrap(exception);
    }
}