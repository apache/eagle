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
package org.apache.eagle.alert.coordinator;

import com.google.common.base.Stopwatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.eagle.alert.config.ZKConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExclusiveExecutor implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ExclusiveExecutor.class);

    private static final int ZK_RETRYPOLICY_SLEEP_TIME_MS = 1000;
    private static final int ZK_RETRYPOLICY_MAX_RETRIES = 3;

    public static final int ACQUIRE_LOCK_WAIT_INTERVAL_MS = 3000;
    public static final int ACQUIRE_LOCK_MAX_RETRIES_TIMES = 100; //about 5 minutes

    private CuratorFramework client;
    private LeaderSelector selector;

    public ExclusiveExecutor(ZKConfig zkConfig ) {
        client = CuratorFrameworkFactory.newClient(
            zkConfig.zkQuorum,
            zkConfig.zkSessionTimeoutMs,
            zkConfig.connectionTimeoutMs,
            new RetryNTimes(ZK_RETRYPOLICY_MAX_RETRIES, ZK_RETRYPOLICY_SLEEP_TIME_MS)
        );
        client.start();
    }

    public void execute(String path, final Runnable r) throws TimeoutException {
        execute(path, r, ACQUIRE_LOCK_MAX_RETRIES_TIMES * ACQUIRE_LOCK_WAIT_INTERVAL_MS);
    }

    public void execute(String path, final Runnable r, int timeoutMillis) throws TimeoutException {
        final AtomicBoolean executed = new AtomicBoolean(false);
        Stopwatch watch = Stopwatch.createUnstarted();
        watch.start();
        LeaderSelectorListener listener = new LeaderSelectorListenerAdapter() {

            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                // this callback will get called when you are the leader
                // do whatever leader work you need to and only exit
                // this method when you want to relinquish leadership
                LOG.info("this is leader node right now..");
                executed.set(true);
                try {
                    r.run();
                } catch (Throwable t) {
                    LOG.warn("failed to run exclusive executor", t);
                }
                LOG.info("leader node executed done!..");
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                LOG.info(String.format("leader selector state change listener, new state: %s", newState.toString()));
            }

        };

        selector = new LeaderSelector(client, path, listener);
        // selector.autoRequeue(); // not required, but this is behavior that you
        // will probably expect
        selector.start();

        // wait for given times
        while (watch.elapsed(TimeUnit.MILLISECONDS) < timeoutMillis) { //about 3 minutes waiting
            if (!executed.get()) {
                try {
                    Thread.sleep(ACQUIRE_LOCK_WAIT_INTERVAL_MS);
                } catch (InterruptedException e) {
                    // ignored
                }
                continue;
            } else {
                break;
            }
        }
        watch.stop();

        if (!executed.get()) {
            throw new TimeoutException(String.format("Get exclusive lock for operation on path %s failed due to wait too much time: %d ms",
                path, watch.elapsed(TimeUnit.MILLISECONDS)));
        }
        LOG.info("Exclusive operation done with execution time (lock plus operation) {} ms !", watch.elapsed(TimeUnit.MILLISECONDS));
    }

    @Override
    public void close() throws IOException {
        if (selector != null) {
            CloseableUtils.closeQuietly(this.selector);
        }
        if (client != null) {
            CloseableUtils.closeQuietly(this.client);
        }
    }

}
