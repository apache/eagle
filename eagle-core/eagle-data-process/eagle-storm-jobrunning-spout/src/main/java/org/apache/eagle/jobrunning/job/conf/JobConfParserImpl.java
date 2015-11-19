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
package org.apache.eagle.jobrunning.job.conf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class JobConfParserImpl implements JobConfParser {
	
	public Map<String, String> parse(Document doc) {
		Elements elements = doc.select("table[id=conf]").select("tbody").select("tr");
		Iterator<Element> iter = elements.iterator();
		Map<String, String> configs = new HashMap<String, String>();
		while(iter.hasNext()) {
			Element element = iter.next();
			Elements tds = element.children();
			String key = tds.get(0).text();
			String value = tds.get(1).text();
			configs.put(key, value);
		}
		return configs;
	}
}
