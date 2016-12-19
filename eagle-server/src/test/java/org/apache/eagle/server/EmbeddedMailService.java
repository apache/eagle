/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.server;

import com.dumbster.smtp.SimpleSmtpServer;
import com.dumbster.smtp.SmtpMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

@Path("/mail")
public class EmbeddedMailService {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedMailService.class);
    private static int SMTP_PORT = 5025;
    private static String SMTP_HOST = "localhost";
    private static SimpleSmtpServer SMTP_SERVER = null;
    private static final String MESSAGET_ID = "Message-ID";

    static {
        try {
            SMTP_HOST = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.error(e.getMessage(), e);
        }
        boolean success = false;
        int attempt = 0;
        while (!success && attempt < 3) {
            try {
                SMTP_PORT = SMTP_PORT + attempt;
                LOGGER.info("Starting Local SMTP service: smtp://{}:{}", SMTP_HOST, SMTP_PORT, attempt);
                SMTP_SERVER = SimpleSmtpServer.start(SMTP_PORT + attempt);
                success = true;
            } catch (Exception ex) {
                LOGGER.warn("Failed to start SMTP service, attempt {}", attempt + 1, ex);
                success = false;
            } finally {
                attempt++;
            }
        }
        if (!success) {
            LOGGER.error("Failed to start SMTP Server, exceeded max attempt times: 3");
            throw new IllegalStateException("Failed to start SMTP Server, exceeded max attempt times: 3");
        }
    }

    @Context
    UriInfo uri;

    @GET
    @Path("")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getMailServiceInfo() throws UnknownHostException {
        String baseUri = uri.getBaseUri().toASCIIString();
        Iterator<SmtpMessage> messageIterator = SMTP_SERVER.getReceivedEmail();
        List<Map<String, Object>> receivedEmails = new ArrayList<>(SMTP_SERVER.getReceivedEmailSize());
        while (messageIterator.hasNext()) {
            receivedEmails.add(convertEmail(messageIterator.next()));
        }

        return new HashMap<String, Object>() {{
            put("smtp_server", createSMTPInfo());
            put("email_size", SMTP_SERVER.getReceivedEmailSize());
            put("emails", receivedEmails);
        }};
    }

    private Map<String, Object> createSMTPInfo() {
        return new HashMap<String, Object>() {{
            put("stopped", SMTP_SERVER.isStopped());
            put("host", SMTP_HOST);
            put("port", SMTP_PORT);
            put("auth", false);
        }};
    }

    private Map<String, Object> convertEmail(SmtpMessage message) {
        String baseUri = uri.getBaseUri().toASCIIString();

        Map<String, String> headers = new HashMap<>();
        message.getHeaderNames().forEachRemaining(headerName -> {
            headers.put((String) headerName, message.getHeaderValue((String) headerName));
        });
        return new HashMap<String, Object>() {{
            put("headers", headers);
            put("body", message.getBody());
            put("urls", new HashMap<String, String>() {{
                put("json_url", baseUri + "mail/email/" + headers.get(MESSAGET_ID) + "?format=json");
                put("html_url", baseUri + "mail/email/" + headers.get(MESSAGET_ID) + "?format=html");
            }});
        }};
    }

    @GET
    @Path("/email/{messageId}")
    public Response getEmailByMessageId(@PathParam("messageId") String messageId, @QueryParam("format") String format) throws UnknownHostException {
        Iterator<SmtpMessage> messageIterator = SMTP_SERVER.getReceivedEmail();
        while (messageIterator.hasNext()) {
            SmtpMessage message = messageIterator.next();
            if (message.getHeaderValue("Message-ID").equals(messageId)) {
                if (format != null && format.equalsIgnoreCase("html")) {
                    return Response.ok(message.getBody()).type(MediaType.TEXT_HTML_TYPE).build();
                } else {
                    return Response.ok(convertEmail(message)).type(MediaType.APPLICATION_JSON_TYPE).build();
                }
            }
        }
        return Response.status(Response.Status.BAD_REQUEST).entity("Unknown Message-ID: " + messageId).build();
    }

    @POST
    @Path("/smtp/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> resetSMTPServer() throws UnknownHostException {
        LOGGER.info("Resetting Local SMTP Server: smtp://{}:{}", SMTP_HOST, SMTP_PORT);
        SMTP_SERVER.stop();
        SMTP_SERVER = SimpleSmtpServer.start(SMTP_PORT);
        return getMailServiceInfo();
    }
}