/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.metadata.utils;

import javax.xml.bind.ValidationEventHandler;
import javax.xml.bind.annotation.DomHandler;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.io.StringWriter;

public abstract class InnerDomAsTextHandler implements DomHandler<String, StreamResult> {
    private final String startTag;
    private final String endTag;
    private StringWriter xmlWriter = new StringWriter();

    /**
     * Default constructor.
     *
     * It is up to a JAXB provider to decide which DOM implementation
     * to use or how that is configured.
     */
    public InnerDomAsTextHandler(String tagName) {
        this.startTag = String.format("<%s>",tagName);
        this.endTag = String.format("</%s>",tagName);
    }

    @Override
    public StreamResult createUnmarshaller(ValidationEventHandler errorHandler) {
        return new StreamResult(xmlWriter);
    }

    @Override
    public String getElement(StreamResult rt) {
        String xml = rt.getWriter().toString();
        int beginIndex = xml.indexOf(startTag) + startTag.length();
        int endIndex = xml.indexOf(endTag);
        return xml.substring(beginIndex, endIndex);
    }

    @Override
    public Source marshal(String n, ValidationEventHandler errorHandler) {
        try {
            StringReader xmlReader = new StringReader(n);
            return new StreamSource(xmlReader);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}