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

package org.apache.eagle.gc.parser;

import org.apache.eagle.gc.model.GCPausedEvent;
import org.apache.eagle.gc.parser.exception.IgnoredLogFormatException;
import org.apache.eagle.gc.parser.exception.UnrecognizedLogFormatException;

public class OtherLogFormatParser implements GCEventParser {

	public OtherLogFormatParser(){
	}
	
	@Override
	public GCPausedEvent parse(String line) throws Exception{
		if(line.contains("CMS-concurrent-mark-start") || line.contains("CMS-concurrent-preclean")
			|| line.contains("CMS-concurrent-abortable-preclean") || line.contains("CMS-concurrent-sweep") || line.contains("CMS-concurrent-reset")) {
			throw new IgnoredLogFormatException("The log is non stop the world event, just ignore it, log: "+ line);
		}
		throw new UnrecognizedLogFormatException("The log pattern is unknown, log: " + line);
	}
}
