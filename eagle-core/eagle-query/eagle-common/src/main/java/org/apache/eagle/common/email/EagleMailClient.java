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
package org.apache.eagle.common.email;

import java.io.File;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConcurrentMapConfiguration;

public class EagleMailClient {
//	private static final String CONFIG_FILE = "config.properties";
	private static final String BASE_PATH = "templates/";
	private static final String AUTH_CONFIG = "mail.smtp.auth";
	private static final String DEBUG_CONFIG = "mail.debug";
	private static final String USER_CONFIG = "mail.user";
	private static final String PWD_CONFIG = "mail.pwd";

	private VelocityEngine velocityEngine;
	private Session session;
	private static final Logger LOG = LoggerFactory.getLogger(EagleMailClient.class);

	public EagleMailClient() {
		this(new ConcurrentMapConfiguration());
	}
	
	public EagleMailClient(AbstractConfiguration configuration) {
		try {
			ConcurrentMapConfiguration con = (ConcurrentMapConfiguration)configuration;
			velocityEngine = new VelocityEngine();
			velocityEngine.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
			velocityEngine.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
			velocityEngine.init();

			con.setProperty("mail.transport.protocol", "smtp");
			final Properties config = con.getProperties();
			if(Boolean.parseBoolean(config.getProperty(AUTH_CONFIG))){
				session = Session.getDefaultInstance(config, new Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(config.getProperty(USER_CONFIG), config.getProperty(PWD_CONFIG));
					}
				});
			}
			else session = Session.getDefaultInstance(config, new Authenticator() {});
			final String debugMode =  config.getProperty(DEBUG_CONFIG, "false");
			final boolean debug =  Boolean.parseBoolean(debugMode);
			session.setDebug(debug);
		} catch (Exception e) {
            LOG.error("Failed connect to smtp server",e);
		}
	}

	private boolean _send(String from, String to, String cc, String title,
			String content) {
		Message msg = new MimeMessage(session);
		try {
			msg.setFrom(new InternetAddress(from));
			msg.setSubject(title);
			if (to != null) {
				msg.setRecipients(Message.RecipientType.TO,
						InternetAddress.parse(to));
			}
			if (cc != null) {
				msg.setRecipients(Message.RecipientType.CC,
						InternetAddress.parse(cc));
			}
			//msg.setRecipients(Message.RecipientType.BCC, InternetAddress.parse(DEFAULT_BCC_ADDRESS));
			msg.setContent(content, "text/html;charset=utf-8");
			LOG.info(String.format("Going to send mail: from[%s], to[%s], cc[%s], title[%s]", from, to, cc, title));
			Transport.send(msg);
			return true;
		} catch (AddressException e) {
			LOG.info("Send mail failed, got an AddressException: " + e.getMessage(), e);
			return false;
		} catch (MessagingException e) {
			LOG.info("Send mail failed, got an AddressException: " + e.getMessage(), e);
			return false;
		}
	}

	private boolean _send(String from,String to,String cc,String title,String content,List<MimeBodyPart> attachments){
		MimeMessage  mail = new MimeMessage(session);
		try {
			mail.setFrom(new InternetAddress(from));
			mail.setSubject(title);
			if (to != null) {
				mail.setRecipients(Message.RecipientType.TO,
						InternetAddress.parse(to));
			}
			if (cc != null) {
				mail.setRecipients(Message.RecipientType.CC,
						InternetAddress.parse(cc));
			}
			
			//mail.setRecipients(Message.RecipientType.BCC, InternetAddress.parse(DEFAULT_BCC_ADDRESS));

			MimeBodyPart mimeBodyPart = new MimeBodyPart();
			mimeBodyPart.setContent(content,"text/html;charset=utf-8");

			Multipart  multipart = new MimeMultipart();
			multipart.addBodyPart(mimeBodyPart);

			for(MimeBodyPart attachment:attachments){
				multipart.addBodyPart(attachment);
			}

			mail.setContent(multipart);
//			mail.setContent(content, "text/html;charset=utf-8");
			LOG.info(String.format("Going to send mail: from[%s], to[%s], cc[%s], title[%s]", from, to, cc, title));
			Transport.send(mail);
			return true;
		} catch (AddressException e) {
			LOG.info("Send mail failed, got an AddressException: " + e.getMessage(), e);
			return false;
		} catch (MessagingException e) {
			LOG.info("Send mail failed, got an AddressException: " + e.getMessage(), e);
			return false;
		}
	}

	public boolean send(String from, String to, String cc, String title,
			String content) {
		return this._send(from, to, cc, title, content);
	}

	public boolean send(String from, String to, String cc, String title,
			String templatePath, VelocityContext context) {
		Template t = null;
		try {
			t = velocityEngine.getTemplate(BASE_PATH + templatePath);
		} catch (ResourceNotFoundException ex) {
		}
		if (t == null) {
			try {
				t = velocityEngine.getTemplate(templatePath);
			} catch (ResourceNotFoundException e) {
				t = velocityEngine.getTemplate("/" + templatePath);
			}
		}
		final StringWriter writer = new StringWriter();
		t.merge(context, writer);
		if(LOG.isDebugEnabled()) LOG.debug(writer.toString());
		return this.send(from, to, cc, title, writer.toString());
	}

	public boolean send(String from, String to, String cc, String title,
	                    String templatePath, VelocityContext context, Map<String,File> attachments) {
		if (attachments == null || attachments.isEmpty()) {
			return send(from, to, cc, title, templatePath, context);
		}
		Template t = null;

		List<MimeBodyPart> mimeBodyParts = new ArrayList<MimeBodyPart>();
		Map<String,String> cid = new HashMap<String,String>();

		for (Map.Entry<String,File> entry : attachments.entrySet()) {
			final String attachment = entry.getKey();
			final File attachmentFile  = entry.getValue();
			final MimeBodyPart mimeBodyPart = new MimeBodyPart();
			if(attachmentFile !=null && attachmentFile.exists()){
				DataSource source = new FileDataSource(attachmentFile);
				try {
					mimeBodyPart.setDataHandler(new DataHandler(source));
					mimeBodyPart.setFileName(attachment);
					mimeBodyPart.setDisposition(MimeBodyPart.ATTACHMENT);
					mimeBodyPart.setContentID(attachment);
					cid.put(attachment,mimeBodyPart.getContentID());
					mimeBodyParts.add(mimeBodyPart);
				} catch (MessagingException e) {
					LOG.error("Generate mail failed, got exception while attaching files: " + e.getMessage(), e);
				}
			}else{
				LOG.error("Attachment: " + attachment + " is null or not exists");
			}
		}
		//TODO remove cid, because not used at all
		if(LOG.isDebugEnabled()) LOG.debug("Cid maps: "+cid);
		context.put("cid", cid);

		try {
			t = velocityEngine.getTemplate(BASE_PATH + templatePath);
		} catch (ResourceNotFoundException ex) {
//			LOGGER.error("Template not found:"+BASE_PATH + templatePath, ex);
		}

		if (t == null) {
			try {
				t = velocityEngine.getTemplate(templatePath);
			} catch (ResourceNotFoundException e) {
				try {
					t = velocityEngine.getTemplate("/" + templatePath);
				}
				catch (Exception ex) {
					LOG.error("Template not found:"+ "/" + templatePath, ex);
				}
			}
		}

		final StringWriter writer = new StringWriter();
		t.merge(context, writer);
		if(LOG.isDebugEnabled()) LOG.debug(writer.toString());
		return this._send(from, to, cc, title, writer.toString(), mimeBodyParts);
	}
}
