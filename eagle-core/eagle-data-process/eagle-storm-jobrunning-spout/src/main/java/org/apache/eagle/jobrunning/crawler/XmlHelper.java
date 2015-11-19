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
package org.apache.eagle.jobrunning.crawler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class XmlHelper {
	
	public static Map<String, String> getConfigs(InputStream is) throws IOException, SAXException, ParserConfigurationException
	{		
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		
		Document dt = db.parse(is);
		Element element = dt.getDocumentElement();
		Map<String, String> config = new TreeMap<String, String>();
				
		NodeList propertyList = element.getElementsByTagName("property");
		int length = propertyList.getLength();
		for(int i = 0; i < length; i++) {
			Node property = propertyList.item(i);
			String key = property.getChildNodes().item(0).getTextContent();
			String value = property.getChildNodes().item(1).getTextContent();
			config.put(key, value);
		}
		return config;		
	}
}
