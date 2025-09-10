/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.mirror.MirrorUtils.adminCall;

/** Uses an include and exclude pattern. */
public class DefaultTopicFilter implements TopicFilter {
    private static final Logger log = LoggerFactory.getLogger(DefaultTopicFilter.class);

    public static final String TOPICS_INCLUDE_CONFIG = "topics";
    private static final String TOPICS_INCLUDE_DOC = "List of topics and/or regexes to replicate.";
    public static final String TOPICS_INCLUDE_DEFAULT = ".*";

    public static final String TOPICS_EXCLUDE_CONFIG = "topics.exclude";
    public static final String TOPICS_EXCLUDE_CONFIG_ALIAS = "topics.blacklist";
    private static final String TOPICS_EXCLUDE_DOC = "List of topics and/or regexes that should not be replicated.";
    public static final String TOPICS_EXCLUDE_DEFAULT = ".*[\\-\\.]internal, .*\\.replica, __.*";

    private Pattern includePattern;
    private Pattern excludePattern;

    private String sourceClusterAlias;
    private ReplicationPolicy replicationPolicy;
    private Admin targetAdminClient;
    private long refreshTopicsIntervalMs = -1;

    private volatile Set<String> targetTopics;
    private volatile long lastRefreshTargetTopicsTimestamp = -1;


    @Override
    public void configure(Map<String, ?> props) {
        TopicFilterConfig config = new TopicFilterConfig(props);
        includePattern = config.includePattern();
        excludePattern = config.excludePattern();

        Map<String, String> taskProps = new HashMap<>();

        props.forEach((key, value) -> {
            taskProps.put(key, (String) value);
        });

        MirrorSourceTaskConfig taskConfig = new MirrorSourceTaskConfig(taskProps);

        sourceClusterAlias = taskConfig.sourceClusterAlias();
        replicationPolicy = taskConfig.replicationPolicy();
        targetAdminClient = taskConfig.forwardingAdmin(taskConfig.targetAdminConfig("topic-filter-target-admin"));
        refreshTopicsIntervalMs = taskConfig.refreshTopicsInterval().minusSeconds(30).toMillis();

        this.refreshTargetTopics();
    }

    private boolean included(String topic) {
        return includePattern != null && includePattern.matcher(topic).matches();
    }

    private boolean excluded(String topic) {
        return excludePattern != null && excludePattern.matcher(topic).matches();
    }

    @Override
    public boolean shouldReplicateTopic(String topic) {
        log.info("判断该主题是否要同步");

        boolean contains = true;

        boolean autoCreateTopicsEnabled = Boolean.parseBoolean(System.getProperty("MM2_AUTO_CREATE_TOPICS_ENABLE", "true"));
        if (!autoCreateTopicsEnabled) {
            String targetTopic = replicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);

            if (targetTopics == null || (refreshTopicsIntervalMs > 0 && System.currentTimeMillis() - lastRefreshTargetTopicsTimestamp > refreshTopicsIntervalMs)) {
                this.refreshTargetTopics();
            }

            contains = targetTopics != null && targetTopics.contains(targetTopic);

            if (!contains) {
                log.info("下游集群没有该主题--{} -> {}", topic, targetTopic);
            }

        }

        return included(topic) && !excluded(topic) && contains;
    }

    @Override
    public void close() {
        log.info("关闭时清理资源--targetAdminClient&targetTopics");

        if (targetAdminClient != null) {
            targetAdminClient.close();
        }

        if (targetTopics != null) {
            synchronized (this) {
                targetTopics.clear();
                targetTopics = null;
            }
        }
    }

    private void refreshTargetTopics() {
        long startTime = System.currentTimeMillis();

        log.info("刷新下游集群主题列表开始");

        try {
            long now = System.currentTimeMillis();
            Set<String> topics = adminCall(() -> targetAdminClient.listTopics().names().get(), () -> "list topics on target cluster");
            synchronized (this) {
                this.targetTopics = topics;
                this.lastRefreshTargetTopicsTimestamp = now;
            }

        } catch (ExecutionException | InterruptedException e) {
            log.warn("获取目标集群主题列表失败", e);
        }

        log.info("刷新下游集群主题列表结束, 耗时 {}", System.currentTimeMillis() - startTime);

    }

    static class TopicFilterConfig extends AbstractConfig {

        static final ConfigDef DEF = new ConfigDef()
            .define(TOPICS_INCLUDE_CONFIG,
                    Type.LIST,
                    TOPICS_INCLUDE_DEFAULT,
                    Importance.HIGH,
                    TOPICS_INCLUDE_DOC)
            .define(TOPICS_EXCLUDE_CONFIG,
                    Type.LIST,
                    TOPICS_EXCLUDE_DEFAULT,
                    Importance.HIGH,
                    TOPICS_EXCLUDE_DOC)
            .define(TOPICS_EXCLUDE_CONFIG_ALIAS,
                    Type.LIST,
                    null,
                    Importance.HIGH,
                    "Deprecated. Use " + TOPICS_EXCLUDE_CONFIG + " instead.");

        TopicFilterConfig(Map<String, ?> props) {
            super(DEF, ConfigUtils.translateDeprecatedConfigs(props, new String[][]{
                {TOPICS_EXCLUDE_CONFIG, TOPICS_EXCLUDE_CONFIG_ALIAS}}), false);
        }

        Pattern includePattern() {
            return MirrorUtils.compilePatternList(getList(TOPICS_INCLUDE_CONFIG));
        }

        Pattern excludePattern() {
            return MirrorUtils.compilePatternList(getList(TOPICS_EXCLUDE_CONFIG));
        }
    }
}
