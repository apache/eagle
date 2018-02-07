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
package org.apache.eagle.log.entity.repo;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.eagle.common.utils.ReflectionsHelper;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.meta.EntitySerDeser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public final class EntityRepositoryScanner {

    private static final Logger LOG = LoggerFactory.getLogger(EntityRepositoryScanner.class);

    // public static void scan() throws InstantiationException, IllegalAccessException {
    // // TODO currently extcos 0.3b doesn't support to search packages like "com.*.eagle.*", "org.*.eagle.*".
    // However 0.4b depends on asm-all version 4.0, which is
    // // conflicted with jersey server 1.8. We should fix it later
    // LOG.info("Scanning all entity repositories with pattern \"org.apache.eagle.*\"");
    // final ComponentScanner scanner = new ComponentScanner();
    // final Set<Class<?>> classes = scanner.getClasses(new EntityRepoScanQuery() );
    // for (Class<?> entityClass : classes) {
    // LOG.info("Processing entity repository: " + entityClass.getName());
    // if (EntityRepository.class.isAssignableFrom(entityClass)) {
    // EntityRepository repo = (EntityRepository)entityClass.newInstance();
    // addRepo(repo);
    // }
    // }
    // }

    public static void scan() throws IllegalAccessException, InstantiationException {
        LOG.info("Scanning all entity repositories");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (Class<? extends EntityRepository> entityRepoClass : ReflectionsHelper.getInstance()
            .getSubTypesOf(EntityRepository.class)) {
            if (EntityRepository.class.isAssignableFrom(entityRepoClass)) {
                EntityRepository repo = entityRepoClass.newInstance();
                addRepo(repo);
            }
        }
        stopWatch.stop();
        LOG.info("Finished scanning entity repositories in {} ms", stopWatch.getTime());
    }

    private static void addRepo(EntityRepository repo) {
        final Map<Class<?>, EntitySerDeser<?>> serDeserMap = repo.getSerDeserMap();
        for (Map.Entry<Class<?>, EntitySerDeser<?>> entry : serDeserMap.entrySet()) {
            EntityDefinitionManager.registerSerDeser(entry.getKey(), entry.getValue());
        }
        final Collection<Class<? extends TaggedLogAPIEntity>> entityClasses = repo.getEntitySet();
        for (Class<? extends TaggedLogAPIEntity> clazz : entityClasses) {
            EntityDefinitionManager.registerEntity(clazz);
        }
    }

    // public static class EntityRepoScanQuery extends ComponentQuery {
    //
    // @Override
    // protected void query() {
    // select().from("org.apache.eagle").returning(
    // allExtending(EntityRepository.class));
    // }
    // }
}
