/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

/**
 * 
 */
package org.apache.eagle.gc.parser.full;

import org.apache.eagle.gc.parser.GCType;
import org.apache.eagle.gc.model.GCPausedEvent;
import org.apache.eagle.gc.parser.DateTimeParser;
import org.apache.eagle.gc.parser.GCEventParser;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class NormalFullGCParser implements GCEventParser {
	List<Pattern> patterns = new ArrayList<Pattern>(); 
	
	/***
	 * 2014-08-13T12:22:25.488-0700: 144.526: [Full GC2014-08-13T12:22:25.488-0700: 144.526: [CMS: 9845647K->10115891K(97517568K), 14.2064400 secs] 10215536K->10115891K(100348736K), [CMS Perm : 24119K->24107K(24320K)], 14.2066090 secs] [Times: user=13.86 sys=0.32, real=14.20 secs]
	 */	
	public NormalFullGCParser() {
		patterns.add(Pattern.compile("^([^\\s]+): (?:[\\.0-9]+): \\[Full GC(?:[^\\s]+): (?:[\\.0-9]+): \\[CMS: ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), (?:[\\.0-9]+) secs\\] ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), \\[CMS Perm : ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\)\\], ([\\.0-9]+) secs\\] \\[Times: user=(?:[\\.0-9]+) sys=(?:[\\.0-9]+), real=([\\.0-9]+) secs\\]"));
	}
	
	@Override
	public GCPausedEvent parse(String line) throws Exception{
		GCPausedEvent bean = null;
		boolean matched = false;
		Matcher m = null;		
		for (Pattern pattern : patterns) {
			m = pattern.matcher(line);
			matched = m.find();
			if (matched) break;
		}
		
		if(matched){
			bean = new GCPausedEvent();
			// date time portion
			String dateTimeString = m.group(1);
			Date d = DateTimeParser.parseDateTimeString(dateTimeString);
			bean.setTimestamp(d.getTime());
			bean.setEventType(GCType.FullGC.name());

			bean.setTenuredAreaGCed(true);
			bean.setTenuredUsedHeapK(Integer.parseInt(m.group(2)));
			bean.setTenuredTotalHeapK(Integer.parseInt(m.group(4)));

			bean.setPermAreaGCed(true);
			bean.setPermUsedHeapK(Integer.parseInt(m.group(8)));
			bean.setPermTotalHeapK(Integer.parseInt(m.group(10)));

			bean.setPausedGCTimeSec(Double.parseDouble(m.group(11)));
			bean.setLogLine(line);
		}
		return bean;
	}
}
