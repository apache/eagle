/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.eagle.alert.engine.publisher.email;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;

public class EagleMailClient {
    private static final Logger LOG = LoggerFactory.getLogger(EagleMailClient.class);
    private static final String BASE_PATH = "templates/";

    private VelocityEngine velocityEngine;
    private Session session;

    public EagleMailClient() {
        this(new Properties());
    }

    public EagleMailClient(final Properties config) {
        try {
            velocityEngine = new VelocityEngine();
            velocityEngine.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
            velocityEngine.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
            velocityEngine.init();

            config.put("mail.transport.protocol", "smtp");
            if (Boolean.parseBoolean(config.getProperty(AlertEmailConstants.CONF_MAIL_AUTH))) {
                session = Session.getInstance(config, new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(
                            config.getProperty(AlertEmailConstants.CONF_AUTH_USER),
                            config.getProperty(AlertEmailConstants.CONF_AUTH_PASSWORD)
                        );
                    }
                });
            } else {
                session = Session.getInstance(config, new Authenticator() {
                });
            }

            final String debugMode = config.getProperty(AlertEmailConstants.CONF_MAIL_DEBUG, "false");
            final boolean debug = Boolean.parseBoolean(debugMode);
            LOG.info("Set email debug mode: " + debugMode);
            session.setDebug(debug);
        } catch (Exception e) {
            LOG.error("Failed to connect to smtp server", e);
        }
    }

    private boolean sendInternal(String from, String to, String cc, String title, String content) {
        Message msg = new MimeMessage(session);
        try {
            msg.setFrom(new InternetAddress(from));
            msg.setSubject(title);
            if (to != null) {
                msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            }
            if (cc != null) {
                msg.setRecipients(Message.RecipientType.CC, InternetAddress.parse(cc));
            }
            //msg.setRecipients(Message.RecipientType.BCC, InternetAddress.parse(DEFAULT_BCC_ADDRESS));
            msg.setContent(content, "text/html;charset=utf-8");
            LOG.info(String.format("Going to send mail: from[%s], to[%s], cc[%s], title[%s]", from, to, cc, title));
            Transport.send(msg);
            return true;
        } catch (AddressException e) {
            LOG.info("Failed to send mail, got an AddressException: " + e.getMessage(), e);
            return false;
        } catch (MessagingException e) {
            LOG.info("Failed to send mail, got an AddressException: " + e.getMessage(), e);
            return false;
        }
    }

    private boolean sendInternal(String from, String to, String cc, String title, String content, List<MimeBodyPart> attachments) {
        MimeMessage mail = new MimeMessage(session);
        try {
            mail.setFrom(new InternetAddress(from));
            mail.setSubject(title);
            if (to != null) {
                mail.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            }
            if (cc != null) {
                mail.setRecipients(Message.RecipientType.CC, InternetAddress.parse(cc));
            }

            //mail.setRecipients(Message.RecipientType.BCC, InternetAddress.parse(DEFAULT_BCC_ADDRESS));

            MimeBodyPart mimeBodyPart = new MimeBodyPart();
            mimeBodyPart.setContent(content, "text/html;charset=utf-8");

            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(mimeBodyPart);

            for (MimeBodyPart attachment : attachments) {
                multipart.addBodyPart(attachment);
            }

            mail.setContent(multipart);
            //  mail.setContent(content, "text/html;charset=utf-8");
            LOG.info(String.format("Going to send mail: from[%s], to[%s], cc[%s], title[%s]", from, to, cc, title));
            Transport.send(mail);
            return true;
        } catch (AddressException e) {
            LOG.info("Failed to send mail, got an AddressException: " + e.getMessage(), e);
            return false;
        } catch (MessagingException e) {
            LOG.info("Failed to send mail, got an AddressException: " + e.getMessage(), e);
            return false;
        }
    }

    public boolean send(String from, String to, String cc, String title,
                        String content) {
        return this.sendInternal(from, to, cc, title, content);
    }

    public boolean send(String from, String to, String cc, String title,
                        String templatePath, VelocityContext context) {
        Template t = null;
        try {
            t = velocityEngine.getTemplate(BASE_PATH + templatePath);
        } catch (ResourceNotFoundException ex) {
            // ignored
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
        if (LOG.isDebugEnabled()) {
            LOG.debug(writer.toString());
        }

        return this.send(from, to, cc, title, writer.toString());
    }

    public boolean send(String from, String to, String cc, String title,
                        String templatePath, VelocityContext context, Map<String, File> attachments) {
        if (attachments == null || attachments.isEmpty()) {
            return send(from, to, cc, title, templatePath, context);
        }
        Template t = null;

        List<MimeBodyPart> mimeBodyParts = new ArrayList<MimeBodyPart>();

        for (Map.Entry<String, File> entry : attachments.entrySet()) {
            final String attachment = entry.getKey();
            final File attachmentFile = entry.getValue();
            final MimeBodyPart mimeBodyPart = new MimeBodyPart();
            if (attachmentFile != null && attachmentFile.exists()) {
                DataSource source = new FileDataSource(attachmentFile);
                try {
                    mimeBodyPart.setDataHandler(new DataHandler(source));
                    mimeBodyPart.setFileName(attachment);
                    mimeBodyPart.setDisposition(MimeBodyPart.ATTACHMENT);
                    mimeBodyPart.setContentID(attachment);
                    mimeBodyParts.add(mimeBodyPart);
                } catch (MessagingException e) {
                    LOG.error("Generate mail failed, got exception while attaching files: " + e.getMessage(), e);
                }
            } else {
                LOG.error("Attachment: " + attachment + " is null or not exists");
            }
        }

        try {
            t = velocityEngine.getTemplate(BASE_PATH + templatePath);
        } catch (ResourceNotFoundException ex) {
            // ignored
        }

        if (t == null) {
            try {
                t = velocityEngine.getTemplate(templatePath);
            } catch (ResourceNotFoundException e) {
                try {
                    t = velocityEngine.getTemplate("/" + templatePath);
                } catch (Exception ex) {
                    LOG.error("Template not found:" + "/" + templatePath, ex);
                }
            }
        }

        final StringWriter writer = new StringWriter();
        t.merge(context, writer);
        if (LOG.isDebugEnabled()) {
            LOG.debug(writer.toString());
        }

        return this.sendInternal(from, to, cc, title, writer.toString(), mimeBodyParts);
    }
}
