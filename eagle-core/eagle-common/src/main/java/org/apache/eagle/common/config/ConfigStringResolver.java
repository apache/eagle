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
package org.apache.eagle.common.config;

import com.typesafe.config.Config;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.text.StrLookup;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class ConfigStringResolver {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigStringResolver.class);

    private final StrSubstitutor substitutor;

    public ConfigStringResolver(Config config) {
        this.substitutor = new StrSubstitutor();
        this.substitutor.setVariableResolver(new ConfigStrLookup(config));
    }

    public String resolve(String templateString) {
        return this.substitutor.replace(templateString);
    }

    public String resolve(InputStream templateStream) throws IOException {
        return this.substitutor.replace(IOUtils.toString(templateStream));
    }

    public InputStream resolveAsStream(InputStream templateStream) throws IOException {
        return IOUtils.toInputStream(this.substitutor.replace(IOUtils.toString(templateStream)));
    }

    private class ConfigStrLookup extends StrLookup<String> {

        private final Config config;

        ConfigStrLookup(Config config) {
            this.config = config;
        }

        @Override
        public String lookup(String key) {
            String value = this.config.getString(key);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Resolve config path: {} = {}", key, value);
            }
            return value;
        }
    }
}