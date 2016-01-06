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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * wrapper class for delta event object which is provided by caller
 */
public class DeltaEventValue implements Serializable {
    private static final long serialVersionUID = -6947238996278788982L;
    private String elementId;
    private Object event;

    public String getElementId() {
        return elementId;
    }

    public void setElementId(String elementId) {
        this.elementId = elementId;
    }

    public Object getEvent() {
        return event;
    }

    public void setEvent(Object event) {
        this.event = event;
    }

    public void writeObject(ObjectOutputStream s) throws IOException {
        s.writeUTF(elementId);
        s.writeObject(event);
    }
    public void readObject(ObjectInputStream s) throws Exception {
        elementId = s.readUTF();
        event = s.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeltaEventValue that = (DeltaEventValue) o;

        if (!elementId.equals(that.elementId)) return false;
        return event.equals(that.event);

    }

    @Override
    public int hashCode() {
        int result = elementId.hashCode();
        result = 31 * result + event.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DeltaEventValue{" +
                "elementId='" + elementId + '\'' +
                ", event=" + event +
                '}';
    }
}
