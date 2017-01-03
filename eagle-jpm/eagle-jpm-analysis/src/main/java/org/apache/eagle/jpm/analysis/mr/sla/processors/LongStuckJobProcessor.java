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

package org.apache.eagle.jpm.analysis.mr.sla.processors;

import com.typesafe.config.Config;
import org.apache.eagle.jpm.analysis.Processor;
import org.apache.eagle.jpm.analysis.mr.MRJobAnalysisEntity;
import org.apache.eagle.jpm.analysis.publisher.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongStuckJobProcessor implements Processor<MRJobAnalysisEntity> {
    private static final Logger LOG = LoggerFactory.getLogger(LongStuckJobProcessor.class);

    private Config config;

    public LongStuckJobProcessor(Config config) {
        this.config = config;
    }

    @Override
    public Result.ProcessorResult process(MRJobAnalysisEntity mrJobAnalysisEntity) {
        return new Result.ProcessorResult(Result.ResultLevel.NOTICE, "");
    }
}
