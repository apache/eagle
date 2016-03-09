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
package org.apache.eagle.alert.notification;

import java.util.concurrent.ThreadPoolExecutor;

import com.typesafe.config.ConfigObject;

public class AlertEmailGeneratorBuilder {
	private AlertEmailGenerator generator;
	private AlertEmailGeneratorBuilder(){
		generator = new AlertEmailGenerator();
	}
	public static AlertEmailGeneratorBuilder newBuilder(){
		return new AlertEmailGeneratorBuilder();
	}
	public AlertEmailGeneratorBuilder withSubject(String subject){
		generator.setSubject(subject);
		return this;
	}
	public AlertEmailGeneratorBuilder withSender(String sender){
		generator.setSender(sender);
		return this;
	}
	public AlertEmailGeneratorBuilder withRecipients(String recipients){
		generator.setRecipients(recipients);
		return this;
	}
	public AlertEmailGeneratorBuilder withTplFile(String tplFile){
		generator.setTplFile(tplFile);
		return this;
	}
	public AlertEmailGeneratorBuilder withEagleProps(ConfigObject eagleProps) {
		generator.setEagleProps(eagleProps);
		return this;
	}
    public AlertEmailGeneratorBuilder withExecutorPool(ThreadPoolExecutor threadPoolExecutor) {
        generator.setExecutorPool(threadPoolExecutor);
        return this;
    }

    public AlertEmailGenerator build(){
		return this.generator;
	}
}
