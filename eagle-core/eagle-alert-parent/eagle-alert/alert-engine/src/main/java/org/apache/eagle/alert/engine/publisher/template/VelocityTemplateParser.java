/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.parser.node.ASTReference;
import org.apache.velocity.runtime.parser.node.ASTprocess;
import org.apache.velocity.runtime.resource.loader.StringResourceLoader;
import org.apache.velocity.runtime.resource.util.StringResourceRepository;
import org.apache.velocity.runtime.visitor.NodeViewMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VelocityTemplateParser {
    private static final Logger LOG = LoggerFactory.getLogger(VelocityTemplateParser.class);
    private static final String TEMPLATE_NAME = "template";
    private final Template template;
    private final ParserNodeVisitor visitor;

    public VelocityTemplateParser(String templateString) throws ParseErrorException {
        VelocityEngine engine = new VelocityEngine();
        engine.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, "org.apache.velocity.runtime.log.Log4JLogChute");
        engine.setProperty("runtime.log.logsystem.log4j.logger", LOG.getName());
        engine.setProperty(Velocity.RESOURCE_LOADER, "string");
        engine.addProperty("string.resource.loader.class", StringResourceLoader.class.getName());
        engine.addProperty("string.resource.loader.repository.static", "false");
        engine.addProperty("runtime.references.strict", "true");
        engine.init();
        StringResourceRepository resourceRepository = (StringResourceRepository) engine.getApplicationAttribute(StringResourceLoader.REPOSITORY_NAME_DEFAULT);
        resourceRepository.putStringResource(TEMPLATE_NAME,templateString);
        template = engine.getTemplate(TEMPLATE_NAME);
        ASTprocess data = (ASTprocess) template.getData();
        visitor = new ParserNodeVisitor();
        data.jjtAccept(visitor, null);
    }

    public List<String> getReferenceNames() {
        return this.visitor.getReferenceNames();
    }

    public Template getTemplate() {
        return template;
    }

    /**
     * @throws MethodInvocationException if required variable is missing in context.
     */
    public void validateContext(Map<String,Object> context) throws MethodInvocationException  {
        VelocityContext velocityContext = new VelocityContext();
        for(Map.Entry<String,Object> entry: context.entrySet()) {
            velocityContext.put(entry.getKey(), entry.getValue());
        }
        template.merge(velocityContext,new StringWriter());
    }

    private class ParserNodeVisitor extends NodeViewMode {
        private List<String> referenceNames = new ArrayList<>();

        @Override
        public Object visit(ASTReference node, Object data) {
            referenceNames.add(node.getRootString());
            return super.visit(node, data);
        }

        public List<String> getReferenceNames() {
            return this.referenceNames;
        }
    }
}