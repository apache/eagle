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

/**
   paraNew Promotion failure patterns
   1) With Tenured And Perm GC part
      a) 2014-07-08T21:52:32.910-0700: 2935883.704: [GC 2935883.704: [ParNew (promotion failed): 2827840K->2824012K(2831168K), 0.8317310 secs]2935884.536: [CMS: 71633438K->38250633K(97517568K), 211.7852880 secs] 74430253K->38250633K(100348736K), [CMS Perm : 54880K->54867K(83968K)], 212.6173060 secs] [Times: user=0.00 sys=214.88, real=212.59 secs]
      b) 2014-09-25T18:25:25.424-0700: 2568209.488: [GC2014-09-25T18:25:25.424-0700: 2568209.488: [ParNew (promotion failed): 4665858K->4627344K(4718592K), 1.2918500 secs]2014-09-25T18:25:26.716-0700: 2568210.780: [CMS: 74802314K->42339736K(95420416K), 230.2937730 secs] 79433462K->42339736K(100139008K), [CMS Perm : 49896K->49865K(83316K)], 231.5926250 secs] [Times: user=0.00 sys=233.73, real=231.56 secs]
   2) Without Tenured pattern
 	  c) 2014-09-16T02:10:29.456-0700: 1732113.520: [GC2014-09-16T02:10:29.456-0700: 1732113.520: [ParNew (promotion failed): 4703469K->4718592K(4718592K), 0.9636440 secs]2014-09-16T02:10:30.420-0700: 1732114.484: [CMS2014-09-16T02:10:48.794-0700: 1732132.858: [CMS-concurrent-mark: 28.139/29.793 secs] [Times: user=214.69 sys=8.41, real=29.79 secs]
      d) 2014-06-05T22:57:29.955-0700: 88580.749: [GC 88580.749: [ParNew (promotion failed): 2809111K->2831168K(2831168K), 0.6941530 secs]88581.443: [CMS2014-06-05T22:57:57.509-0700: 88608.303: [CMS-concurrent-sweep: 48.562/51.183 secs] [Times: user=138.07 sys=15.38, real=51.18 secs]
 */
public class ParaNewPromotionFailureParser implements GCEventParser {
	
	List<Pattern> withTenuredGCPatterns = new ArrayList<Pattern>();
	List<Pattern> withoutTenuredGCPatterns = new ArrayList<Pattern>();

	public ParaNewPromotionFailureParser() {
		withTenuredGCPatterns.add(Pattern.compile("^([^\\s]+): (?:[\\.0-9]+): \\[GC (?:[\\.0-9]+): \\[ParNew \\(promotion failed\\): ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), (?:[\\.0-9]+) secs\\](?:[\\.0-9]+): \\[CMS: ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), (?:[\\.0-9]+) secs\\] (?:[0-9]+)K->(?:[0-9]+)K\\((?:[0-9]+)K\\), \\[CMS Perm : ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\)\\], ([\\.0-9]+) secs\\] \\[Times: user=(?:[\\.0-9]+) sys=(?:[\\.0-9]+), real=([\\.0-9]+) secs\\]"));
		withTenuredGCPatterns.add(Pattern.compile("^([^\\s]+): (?:[\\.0-9]+): \\[GC(?:[^\\s]+): (?:[\\.0-9]+): \\[ParNew \\(promotion failed\\): ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), (?:[\\.0-9]+) secs\\](?:[^\\s]+): (?:[\\.0-9]+): \\[CMS: ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), (?:[\\.0-9]+) secs\\] (?:[0-9]+)K->(?:[0-9]+)K\\((?:[0-9]+)K\\), \\[CMS Perm : ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\)\\], ([\\.0-9]+) secs\\] \\[Times: user=(?:[\\.0-9]+) sys=(?:[\\.0-9]+), real=([\\.0-9]+) secs\\]"));

		withoutTenuredGCPatterns.add(Pattern.compile("^([^\\s]+): (?:[\\.0-9]+): \\[GC (?:[\\.0-9]+): \\[ParNew \\(promotion failed\\): ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), (?:[\\.0-9]+) secs\\](?:[\\.0-9]+): \\[CMS(?:[^\\s]+): (?:[\\.0-9]+): \\[(?:[^\\s]+): (?:[\\.0-9]+)/(?:[\\.0-9]+) secs\\] \\[Times: user=(?:[\\.0-9]+) sys=(?:[\\.0-9]+), real=([\\.0-9]+) secs\\]"));
		withoutTenuredGCPatterns.add(Pattern.compile("^([^\\s]+): (?:[\\.0-9]+): \\[GC(?:[^\\s]+): (?:[\\.0-9]+): \\[ParNew \\(promotion failed\\): ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), (?:[\\.0-9]+) secs\\](?:[^\\s]+): (?:[\\.0-9]+): \\[(?:CMS[^\\s]+): (?:[\\.0-9]+): \\[(?:[^\\s]+): (?:[\\.0-9]+)/(?:[\\.0-9]+) secs\\] \\[Times: user=(?:[\\.0-9]+) sys=(?:[\\.0-9]+), real=([\\.0-9]+) secs\\]"));
	}
	
	@Override
	public GCPausedEvent parse(String line) throws Exception{
		GCPausedEvent bean = null;
		boolean matched = false;
		Matcher m = null;
		for (Pattern pattern : withTenuredGCPatterns) {
			m = pattern.matcher(line);
			matched = m.find();
			if (matched) break;
		}
		if(matched){
			bean = new GCPausedEvent();
			String dateTimeString = m.group(1);
			Date d = DateTimeParser.parseDateTimeString(dateTimeString);
			bean.setTimestamp(d.getTime());
			bean.setEventType(GCType.FullGC.name());

			bean.setYoungAreaGCed(true);
			bean.setYoungUsedHeapK(Integer.parseInt(m.group(2)));
			bean.setYoungTotalHeapK(Integer.parseInt(m.group(4)));
			bean.setTenuredAreaGCed(true);
			bean.setTenuredUsedHeapK(Integer.parseInt(m.group(5)));
			bean.setTenuredTotalHeapK(Integer.parseInt(m.group(7)));
			bean.setPermAreaGCed(true);
			bean.setPermUsedHeapK(Integer.parseInt(m.group(8)));
			bean.setPermTotalHeapK(Integer.parseInt(m.group(10)));
			bean.setPausedGCTimeSec(Double.parseDouble(m.group(11)));

			bean.setLogLine(line);
			return bean;
		}

		for (Pattern pattern : withoutTenuredGCPatterns) {
			m = pattern.matcher(line);
			matched = m.find();
			if (matched) break;
		}
		if(matched){
			bean = new GCPausedEvent();
			String dateTimeString = m.group(1);
			Date d = DateTimeParser.parseDateTimeString(dateTimeString);
			bean.setTimestamp(d.getTime());
			bean.setYoungAreaGCed(true);
			bean.setYoungUsedHeapK(Integer.parseInt(m.group(2)));
			bean.setYoungTotalHeapK(Integer.parseInt(m.group(4)));
			bean.setPausedGCTimeSec(Double.parseDouble(m.group(5)));
			bean.setLogLine(line);
			return bean;
		}
		return bean;
	}
}