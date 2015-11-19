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
package org.apache.eagle.alert.notification;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.common.AlertEmailSender;
import org.apache.eagle.alert.email.AlertEmailComponent;
import org.apache.eagle.alert.email.AlertEmailContext;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import com.typesafe.config.ConfigObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertEmailGenerator{
	private String tplFile;
	private String sender;
	private String recipients;
	private String subject;
	private ConfigObject eagleProps;

    private ThreadPoolExecutor executorPool;

    private final static Logger LOG = LoggerFactory.getLogger(AlertEmailGenerator.class);

    private final static long MAX_TIMEOUT_MS =60000;

    public void sendAlertEmail(AlertAPIEntity entity) {
		sendAlertEmail(entity, recipients, null);
	}
	
	public void sendAlertEmail(AlertAPIEntity entity, String recipients) {
		sendAlertEmail(entity, recipients, null);	
	}
	
	public void sendAlertEmail(AlertAPIEntity entity, String recipients, String cc) {
		AlertEmailContext email = new AlertEmailContext();
		
		AlertEmailComponent component = new AlertEmailComponent();
		component.setAlertContext(entity.getAlertContext());
		List<AlertEmailComponent> components = new ArrayList<AlertEmailComponent>();
		components.add(component);		
		email.setComponents(components);
		if (entity.getAlertContext().getProperty(AlertConstants.SUBJECT) != null) {
			email.setSubject(entity.getAlertContext().getProperty(AlertConstants.SUBJECT));
		}
		else email.setSubject(subject);
		email.setVelocityTplFile(tplFile);
		email.setRecipients(recipients);
		email.setCc(cc);
		email.setSender(sender);
		
		/** asynchronized email sending */
		@SuppressWarnings("rawtypes")
        AlertEmailSender thread = new AlertEmailSender(email, eagleProps);

        if(this.executorPool == null) throw new IllegalStateException("Invoking thread executor pool but it's is not set yet");

        LOG.info("Sending email  in asynchronous to: "+recipients+", cc: "+cc);
        Future future = this.executorPool.submit(thread);
        try {
            future.get(MAX_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            LOG.info(String.format("Successfully send email to %s", recipients));
        } catch (InterruptedException | ExecutionException  e) {
            LOG.error(String.format("Failed to send email to %s, due to:%s",recipients,e),e);
        } catch (TimeoutException e) {
            LOG.error(String.format("Failed to send email to %s due to timeout exception, max timeout: %s ms ",recipients, MAX_TIMEOUT_MS),e);
        }
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

    public void setExecutorPool(ThreadPoolExecutor executorPool) {
        this.executorPool = executorPool;
    }
}