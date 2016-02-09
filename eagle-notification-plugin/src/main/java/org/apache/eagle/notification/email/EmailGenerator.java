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
/**
 * 
 */
package org.apache.eagle.notification.email;

import com.typesafe.config.ConfigObject;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.policy.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Email Generator API which creates
 */
public class EmailGenerator {

	private String tplFile;
	private String sender;
	private String recipients;
	private String subject;
	private ConfigObject eagleProps;

	private ExecutorService  _service = Executors.newFixedThreadPool(4); // Revisit this
	private final static Logger LOG = LoggerFactory.getLogger(EmailGenerator.class);

    private final static long MAX_TIMEOUT_MS =60000;

    public boolean sendAlertEmail(AlertAPIEntity entity) {
		return sendAlertEmail(entity, recipients, null);
	}
	
	public boolean sendAlertEmail(AlertAPIEntity entity, String recipients) {
		return sendAlertEmail(entity, recipients, null);
	}
	
	public boolean sendAlertEmail(AlertAPIEntity entity, String recipients, String cc) {
		EmailContext email = new EmailContext();
		boolean sentSuccessfully = false;
		
		EmailComponent component = new EmailComponent();
		component.setAlertContext(entity.getAlertContext());
		List<EmailComponent> components = new ArrayList<EmailComponent>();
		components.add(component);		
		email.setComponents(components);
		if (entity.getAlertContext().getProperty(Constants.SUBJECT) != null) {
			email.setSubject(entity.getAlertContext().getProperty(Constants.SUBJECT));
		}
		else email.setSubject(subject);
		email.setVelocityTplFile(tplFile);
		email.setRecipients(recipients);
		email.setCc(cc);
		email.setSender(sender);
		
		EmailSender emailTask = new EmailSender(email, eagleProps);

		LOG.info("Sending email  in asynchronous to: "+recipients+", cc: "+cc);
		Future fut =  _service.submit(emailTask);
		try {
			fut.get(MAX_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			sentSuccessfully = true;
			LOG.info(String.format("Successfully send email to %s", recipients));
		} catch (InterruptedException | ExecutionException  e) {
			LOG.error(String.format("Failed to send email to %s, due to:%s",recipients,e),e);
		} catch (TimeoutException e) {
			LOG.error(String.format("Failed to send email to %s due to timeout exception, max timeout: %s ms ",recipients, MAX_TIMEOUT_MS),e);
		}
		return sentSuccessfully;
    }
	
	public String getTplFile() {
		return tplFile;
	}
	
	public void setTplFile(String tplFile) {
		this.tplFile = tplFile;
	}

	public String getSender() {
		return sender;
	}

	public void setSender(String sender) {
		this.sender = sender;
	}

	public String getRecipients() {
		return recipients;
	}

	public void setRecipients(String recipients) {
		this.recipients = recipients;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public ConfigObject getEagleProps() {
		return eagleProps;
	}

	public void setEagleProps(ConfigObject eagleProps) {
		this.eagleProps = eagleProps;
	}

}