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

package org.apache.eagle.notification.email;

import com.typesafe.config.ConfigObject;

/**
 * Email Builder API which helps to build EmailGen
 */
public class EmailBuilder {


	private EmailGenerator generator;
	private EmailBuilder(){
		generator = new EmailGenerator();
	}
	public static EmailBuilder newBuilder(){
		return new EmailBuilder();
	}

	public EmailBuilder withSubject(String subject){
		generator.setSubject(subject);
		return this;
	}
	public EmailBuilder withSender(String sender){
		generator.setSender(sender);
		return this;
	}
	public EmailBuilder withRecipients(String recipients){
		generator.setRecipients(recipients);
		return this;
	}
	public EmailBuilder withTplFile(String tplFile){
		generator.setTplFile(tplFile);
		return this;
	}
	public EmailBuilder withEagleProps(ConfigObject eagleProps) {
		generator.setEagleProps(eagleProps);
		return this;
	}

	public EmailGenerator build(){
		return this.generator;
	}
}
