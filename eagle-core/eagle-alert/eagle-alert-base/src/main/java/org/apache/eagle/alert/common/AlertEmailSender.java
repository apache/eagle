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
package org.apache.eagle.alert.common;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.email.AlertEmailContext;
import org.apache.velocity.VelocityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.alert.email.AlertEmailComponent;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.email.EagleMailClient;
import com.netflix.config.ConcurrentMapConfiguration;
import com.typesafe.config.ConfigObject;

public class AlertEmailSender implements Runnable {
	
	protected final List<Map<String, String>> alertContexts = new ArrayList<Map<String, String>>();
	protected final String configFileName;
	protected final String subject;
	protected final String sender;
	protected final String recipents;
	protected final String cc;
	protected final String origin;
	protected boolean sentSuccessfully = false;

	private final static Logger LOG = LoggerFactory.getLogger(AlertEmailSender.class);
	private final static int MAX_RETRY_COUNT = 3;
		
	private static final String MAIL_HOST = "mail.host";	
	private static final String MAIL_PORT = "mail.smtp.port";
	private static final String MAIL_DEBUG = "mail.debug";
	
	private static final String CONF_KEY_MAIL_HOST = "mailHost";	
	private static final String CONF_KEY_MAIL_PORT = "mailSmtpPort";
	private static final String CONF_KEY_MAIL_DEBUG = "mailDebug";

	private ConfigObject eagleProps;


    private String threadName;
	/**
	 * Derived class may have some additional context properties to add
	 * @param context velocity context
	 * @param env environment
	 */
	protected void additionalContext(VelocityContext context, String env) {
		// By default there's no additional context added
	}

	public AlertEmailSender(AlertEmailContext alertEmail){
		this.recipents = alertEmail.getRecipients();
		this.configFileName = alertEmail.getVelocityTplFile();
		this.subject = alertEmail.getSubject();
		this.sender = alertEmail.getSender();
		this.cc = alertEmail.getCc();
		for(AlertEmailComponent bean : alertEmail.getComponents()){
			this.alertContexts.add(bean.getAlertContext().getProperties());
		}
		String tmp = ManagementFactory.getRuntimeMXBean().getName();
		this.origin = tmp.split("@")[1] + "(pid:" + tmp.split("@")[0] + ")";
        threadName = Thread.currentThread().getName();
		LOG.info("Initialized "+threadName+": origin is : " + this.origin+", recipient of the email: " + this.recipents+", velocity TPL file: " + this.configFileName);
	}

	public AlertEmailSender(AlertEmailContext alertEmail, ConfigObject eagleProps){
		this(alertEmail);
		this.eagleProps = eagleProps;
	}

	@Override
	public void run() {
		int count = 0;
		boolean success = false;
		while(count++ < MAX_RETRY_COUNT && !success){
			LOG.info("Sending email, tried: " + count+", max: "+MAX_RETRY_COUNT);
			try {
				final EagleMailClient client;
				if (eagleProps != null) {
					ConcurrentMapConfiguration con = new ConcurrentMapConfiguration();					
					con.addProperty(MAIL_HOST, eagleProps.get(CONF_KEY_MAIL_HOST).unwrapped());
					con.addProperty(MAIL_PORT, eagleProps.get(CONF_KEY_MAIL_PORT).unwrapped());
					if (eagleProps.get(CONF_KEY_MAIL_DEBUG) != null) {
						con.addProperty(MAIL_DEBUG, eagleProps.get(CONF_KEY_MAIL_DEBUG).unwrapped());
					}
					client = new EagleMailClient(con);
				}
				else {
					client = new EagleMailClient();
				}
				String env = "prod";
				if (eagleProps != null && eagleProps.get("env") != null) {
					env = (String) eagleProps.get("env").unwrapped();
				}
				LOG.info("Env is: " + env);
				final VelocityContext context = new VelocityContext();
				generateCommonContext(context);
				LOG.info("After calling generateCommonContext...");
				additionalContext(context, env);
				
				if (recipents == null || recipents.equals("")) {
					LOG.error("Recipients is null, skip sending emails ");
					return;
				}
				String title = subject;
				if (!env.trim().equals("prod")) {
					title = "[" + env + "]" + title; 				
				}
				success = client.send(sender, recipents, cc, title, configFileName, context, null);
				LOG.info("Success of sending email: " + success);
				if(!success && count < MAX_RETRY_COUNT) {
					LOG.info("Sleep for a while before retrying");
					Thread.sleep(10*1000);
				}
			}
			catch (Exception e){
				LOG.warn("Sending mail exception", e);
			}
		}

		if(success){
			sentSuccessfully = true;
            LOG.info(String.format("Successfully send email, thread: %s",threadName));
		}else{
			LOG.warn(String.format("Fail sending email after tries %s times, thread: %s",MAX_RETRY_COUNT,threadName));
		}
	}
	
	private void generateCommonContext(VelocityContext context) {
		context.put(AlertConstants.ALERT_EMAIL_TIME_PROPERTY, DateTimeUtil.millisecondsToHumanDateWithSeconds( System.currentTimeMillis() ));
		context.put(AlertConstants.ALERT_EMAIL_COUNT_PROPERTY, alertContexts.size());
		context.put(AlertConstants.ALERT_EMAIL_ALERTLIST_PROPERTY, alertContexts);
		context.put(AlertConstants.ALERT_EMAIL_ORIGIN_PROPERTY, origin);
	}

	public boolean sentSuccessfully(){
		return this.sentSuccessfully;
	}
}