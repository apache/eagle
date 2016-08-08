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
package org.apache.eagle.app;

import com.typesafe.config.Config;
import org.apache.eagle.app.environment.Environment;
import org.apache.eagle.app.environment.ExecutionRuntimeManager;
import org.apache.eagle.app.utils.ApplicationConfigHelper;

import java.lang.reflect.ParameterizedType;
import java.util.Map;

abstract class AbstractApplication<Conf extends Configuration,Env extends Environment,Proc> implements Application<Conf,Env,Proc>, ApplicationTool<Conf> {
    private Class<Conf> parametrizedConfigClass;

    @Override
    public Proc execute(Map<String, Object> config, Env env) {
        return execute(ApplicationConfigHelper.convertFrom(config, getConfigType()),env);
    }

    @Override
    public Proc execute(Config config, Env environment){
        return null;
    }
    /**
     *  Map application configuration from environment
     *
     * @param config
     * @return
     */
    private Conf loadAppConfigFromEnv(Config config){
        return ApplicationConfigHelper.convertFrom(ApplicationConfigHelper.unwrapFrom(config,getClass().getCanonicalName()), getConfigType());
    }

    @Override
    public void run(Config config) {
//        ExecutionRuntimeManager.getInstance().getRuntime(getEnvironmentType(),config).start(this,loadAppConfigFromEnv(config));
        ExecutionRuntimeManager.getInstance().getRuntime(getEnvironmentType(),config).start(this,config);
    }

    @Override
    public void run(Configuration conf, Config config) {
//        ExecutionRuntimeManager.getInstance().getRuntime(getEnvironmentType(), config).start(this,conf);
    }

    @Override
    public Proc execute(Env environment) {
        return execute(loadAppConfigFromEnv(environment.config()),environment);
    }

    /**
     * @return Config class from Generic Type
     */
    public Class<Conf> getConfigType(){
        if (parametrizedConfigClass == null) {
            this.parametrizedConfigClass = (Class<Conf>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        }
        return parametrizedConfigClass;
    }
}
