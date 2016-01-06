/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.state.deltaevent;

import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * a combination of fields which consists of the key a delta event
 */
public class DeltaEventKey implements Serializable {
    private String site;
    private String applicationId;
    private String elementId;

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public String getElementId() {
        return elementId;
    }

    public void setElementId(String elementId) {
        this.elementId = elementId;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @Override
    public int hashCode(){
        return new HashCodeBuilder().append(site).append(applicationId).append(elementId).toHashCode();
    }

    @Override
    public boolean equals(Object that){
        if(this == that)
            return true;
        if(that == null && !(that instanceof DeltaEventKey))
            return false;
        DeltaEventKey thatKey = (DeltaEventKey)that;
        if(thatKey.applicationId.equals(this.applicationId) &&
                thatKey.elementId.equals(this.elementId) &&
                thatKey.site.equals(this.site))
            return true;
        return false;
    }

    /**
     * serialize this object into output stream
     * @param s
     */
    private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException{
        s.writeUTF(site);
        s.writeUTF(applicationId);
        s.writeUTF(elementId);
    }

    private void readObject(final java.io.ObjectInputStream s) throws java.io.IOException{
        site = s.readUTF();
        applicationId = s.readUTF();
        elementId = s.readUTF();
    }

    @Override
    public String toString() {
        return "DeltaEventKey{" +
                "site='" + site + '\'' +
                ", applicationId='" + applicationId + '\'' +
                ", elementId='" + elementId + '\'' +
                '}';
    }
}
