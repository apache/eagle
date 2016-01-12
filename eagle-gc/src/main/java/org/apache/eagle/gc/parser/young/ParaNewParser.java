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

package org.apache.eagle.gc.parser.young;

import org.apache.eagle.gc.parser.GCType;
import org.apache.eagle.gc.model.GCPausedEvent;
import org.apache.eagle.gc.parser.DateTimeParser;
import org.apache.eagle.gc.parser.GCEventParser;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParaNewParser implements GCEventParser {
	/**
	   a) 2014-06-04T22:21:19.158-0700: 9.952: [GC 9.952: [ParNew: 2365777K->5223K(2831168K), 0.0155080 secs] 2365777K->5223K(100348736K), 0.0156030 secs] [Times: user=0.08 sys=0.05, real=0.02 secs]
	   b) 2014-08-27T01:17:36.211-0700: 940.275: [GC2014-08-27T01:17:36.212-0700: 940.275: [ParNew: 4718592K->524288K(4718592K), 1.1674000 secs] 53266668K->49584240K(100139008K), 1.1676470 secs] [Times: user=19.35 sys=0.26, real=1.16 secs]
	**/
		
	List<Pattern> patterns = new ArrayList<Pattern>(); 
	
	public ParaNewParser() {
		patterns.add(Pattern.compile("^([^\\s]+): (?:[\\.0-9]+): \\[GC (?:[\\.0-9]+): \\[ParNew: ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), ([\\.0-9]+) secs\\] ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), ([\\.0-9]+) secs\\] \\[Times: user=([\\.0-9]+) sys=([\\.0-9]+), real=([\\.0-9]+) secs\\]"));
		patterns.add(Pattern.compile("^([^\\s]+): (?:[\\.0-9]+): \\[GC(?:[^\\s]+): (?:[\\.0-9]+): \\[ParNew: ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), ([\\.0-9]+) secs\\] ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), ([\\.0-9]+) secs\\] \\[Times: user=([\\.0-9]+) sys=([\\.0-9]+), real=([\\.0-9]+) secs\\]"));
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
			bean.setEventType(GCType.YoungGC.name());

			bean.setYoungAreaGCed(true);
			bean.setYoungUsedHeapK(Integer.parseInt(m.group(2)));
			bean.setYoungTotalHeapK(Integer.parseInt(m.group(4)));
			bean.setTotalHeapUsageAvailable(true);
			bean.setUsedTotalHeapK(Integer.parseInt(m.group(6)));
			bean.setTotalHeapK(Integer.parseInt(m.group(8)));

			bean.setPausedGCTimeSec(Double.parseDouble(m.group(9)));
			bean.setLogLine(line);
		}
		return bean;
	}
}
