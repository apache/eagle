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
package org.apache.eagle.app.environment;

import org.apache.eagle.app.Application;
import com.typesafe.config.Config;
import org.apache.eagle.metadata.model.ApplicationEntity;

/**
 * Execution Runtime Adapter.
 */
public interface ExecutionRuntime<E extends Environment, P> {

    /**
     * Prepare runtime.
     *
     * @param environment environment context
     */
    void prepare(E environment);

    E environment();

    /**
     * Start application process.
     *
     * @param executor
     * @param config
     */
    void start(Application<E, P> executor, Config config);

    /**
     * Stop application process.
     *
     * @param executor
     * @param config
     */
    void stop(Application<E, P> executor, Config config);

    /**
     * Check application process status.
     *
     * @param executor
     * @param config
     * @return status
     */
    ApplicationEntity.Status status(Application<E, P> executor, Config config);
}
