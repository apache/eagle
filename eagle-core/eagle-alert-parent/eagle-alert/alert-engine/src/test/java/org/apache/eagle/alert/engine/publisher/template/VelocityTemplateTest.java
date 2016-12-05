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
package org.apache.eagle.alert.engine.publisher.template;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.parser.node.ASTReference;
import org.apache.velocity.runtime.parser.node.ASTprocess;
import org.apache.velocity.runtime.resource.loader.StringResourceLoader;
import org.apache.velocity.runtime.resource.util.StringResourceRepository;
import org.apache.velocity.runtime.visitor.NodeViewMode;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class VelocityTemplateTest {
    private static final Logger LOG = LoggerFactory.getLogger(VelocityTemplateTest.class);

    @Test
    public void testVelocityTemplate() {
        String templateString = "This alert ($category) was generated because $reason and $REASON from $source at $created_time";
        String resultString = "This alert ($category) was generated because timeout and IO error from localhost at 2016-11-30 05:52:47,053";
        VelocityEngine engine = new VelocityEngine();
        engine.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, "org.apache.velocity.runtime.log.Log4JLogChute");
        engine.setProperty("runtime.log.logsystem.log4j.logger", LOG.getName());
        engine.setProperty(Velocity.RESOURCE_LOADER, "string");
        engine.addProperty("string.resource.loader.class", StringResourceLoader.class.getName());
        engine.addProperty("string.resource.loader.repository.static", "false");
        // engine.addProperty("runtime.references.strict", "true");
        engine.init();

        StringResourceRepository repo = (StringResourceRepository) engine.getApplicationAttribute(StringResourceLoader.REPOSITORY_NAME_DEFAULT);
        repo.putStringResource("alert_template", "");
        repo.putStringResource("alert_template", templateString);

        Assert.assertEquals(templateString, repo.getStringResource("alert_template").getBody());

        VelocityContext context = new VelocityContext();
        context.put("reason", "timeout");
        context.put("REASON", "IO error");
        context.put("source","localhost");
        context.put("created_time", "2016-11-30 05:52:47,053");

        Template velocityTemplate = engine.getTemplate("alert_template");
        ASTprocess data = (ASTprocess) velocityTemplate.getData();
        ReferenceContext referenceContext = new ReferenceContext();
        data.jjtAccept(referenceContext,null);
        Assert.assertEquals(5, referenceContext.getReferences().size());
        StringWriter writer = new StringWriter();
        velocityTemplate.merge(context, writer);
        velocityTemplate.process();
        Assert.assertEquals(resultString, writer.toString());
    }

    private class ReferenceContext extends NodeViewMode {
        private List<ASTReference> references = new ArrayList<>();

        @Override
        public Object visit(ASTReference node, Object data) {
            references.add(node);
            return super.visit(node, data);
        }

        public List<ASTReference> getReferences() {
            return this.references;
        }

        public List<String> getReferenceNames() {
            return this.references.stream().map(ASTReference::getRootString).collect(Collectors.toList());
        }
    }
}
