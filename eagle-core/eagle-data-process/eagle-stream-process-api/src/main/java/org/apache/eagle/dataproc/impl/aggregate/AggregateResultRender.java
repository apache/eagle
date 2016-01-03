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
package org.apache.eagle.dataproc.impl.aggregate;

import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.aggregate.entity.AggregateDefinitionAPIEntity;
import org.apache.eagle.dataproc.impl.aggregate.entity.AggregateEntity;
import org.apache.eagle.policy.PolicyEvaluationContext;
import org.apache.eagle.policy.ResultRender;

import java.io.Serializable;
import java.util.List;

/**
 * Created on 12/29/15.
 */
public class AggregateResultRender implements ResultRender<AggregateDefinitionAPIEntity, AggregateEntity>, Serializable {


    @Override
    public AggregateEntity render(Config config,
                                  List<Object> rets,
                                  PolicyEvaluationContext<AggregateDefinitionAPIEntity, AggregateEntity> siddhiAlertContext,
                                  long timestamp) {
        AggregateEntity result = new AggregateEntity();
        for (Object o : rets) {
            result.add(o);
        }
        return result;
    }
}
