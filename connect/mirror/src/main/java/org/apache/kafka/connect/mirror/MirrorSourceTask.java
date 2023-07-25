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

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.concurrent.Semaphore;
import java.time.Duration;

import static org.apache.kafka.connect.mirror.SFMirrorMakerConstants.*;

/** Replicates a set of topic-partitions. */
public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);

    private static final int MAX_OUTSTANDING_OFFSET_SYNCS = 10;

    private KafkaConsumer<byte[], byte[]> consumer;
    private KafkaProducer<byte[], byte[]> offsetProducer;
    private String sourceClusterAlias;
    private String targetClusterAlias;
    private String offsetSyncsTopic;
    private Duration pollTimeout;
    private long maxOffsetLag;
    private Map<TopicPartition, PartitionState> partitionStates;
    private ReplicationPolicy replicationPolicy;
    private MirrorSourceMetrics metrics;
    private boolean stopping = false;
    private final Map<TopicPartition, OffsetSync> pendingOffsetSyncs = new LinkedHashMap<>();
    private Semaphore outstandingOffsetSyncs;
    private Semaphore consumerAccess;

    // 该woker运行的主题分区列表
    private Set<TopicPartition> taskTopicPartitions;

    // 上游集群zk连接串
    private String sourceClusterZkServers;
    // 上游集群消费组zk客户端
    private CuratorFramework sourceZkClient;
    private final RetryPolicy ZK_RETRY_POLICY = new BoundedExponentialBackoffRetry(100, 10000, 10);


    // 同步消费组名称
    private String sfMm2ConsumerGroupId;
    // 循环同步消息头检测开关
    private boolean provenanceHeaderEnable = false;

    private static final JsonMapper JSON_MAPPER = JsonMapper.builder().build();

    // 消费组clientId
    private static Method getClientIdMethod;

    static {
        try {
            getClientIdMethod = KafkaConsumer.class.getDeclaredMethod("getClientId");
        } catch (NoSuchMethodException e) {
            log.error("获取消费组客户端id报错", e);
        }
        getClientIdMethod.setAccessible(true);
    }

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy, long maxOffsetLag, KafkaProducer<byte[], byte[]> producer,
                     Semaphore outstandingOffsetSyncs, Map<TopicPartition, PartitionState> partitionStates,
                     String offsetSyncsTopic) {
        this.consumer = consumer;
        this.metrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        this.maxOffsetLag = maxOffsetLag;
        consumerAccess = new Semaphore(1);
        this.offsetProducer = producer;
        this.outstandingOffsetSyncs = outstandingOffsetSyncs;
        this.partitionStates = partitionStates;
        this.offsetSyncsTopic = offsetSyncsTopic;
    }

    @Override
    public void start(Map<String, String> props) {
        sfMm2ConsumerGroupId = System.getProperty(MM2_CONSUMER_GROUP_ID_KEY);
        provenanceHeaderEnable = Boolean
                .valueOf(System.getProperty(PROVENANCE_HEADER_ENABLE_KEY, Boolean.FALSE.toString()));
        String targetClusterZkServers = props.get(TARGET_CLUSTER_ZOOKEEPER_SERVERS);

        if (Strings.isNullOrEmpty(props.get(SOURCE_CLUSTER_ZOOKEEPER_SERVERS))
                || Strings.isNullOrEmpty(props.get(TARGET_CLUSTER_ZOOKEEPER_SERVERS))) {
            log.error("上下游集群ZK为必配项！");
            Exit.exit(4);
        }

        // 循环同步检测
        checkBidirectionSync(targetClusterZkServers, sfMm2ConsumerGroupId);

        // 上游zk初始化
        sourceClusterZkServers = props.get(SOURCE_CLUSTER_ZOOKEEPER_SERVERS);
        sourceZkClient = CuratorFrameworkFactory.newClient(sourceClusterZkServers, ZK_RETRY_POLICY);
        sourceZkClient.start();

        MirrorSourceTaskConfig config = new MirrorSourceTaskConfig(props);
        pendingOffsetSyncs.clear();
        outstandingOffsetSyncs = new Semaphore(MAX_OUTSTANDING_OFFSET_SYNCS);
        consumerAccess = new Semaphore(1);  // let one thread at a time access the consumer
        sourceClusterAlias = config.sourceClusterAlias();
        targetClusterAlias = config.targetClusterAlias();
        metrics = config.metrics();
        pollTimeout = config.consumerPollTimeout();
        maxOffsetLag = config.maxOffsetLag();
        replicationPolicy = config.replicationPolicy();
        partitionStates = new HashMap<>();
        offsetSyncsTopic = config.offsetSyncsTopic();
        consumer = MirrorUtils.newConsumer(config.sourceConsumerConfig("replication-consumer"));
        offsetProducer = MirrorUtils.newProducer(config.offsetSyncsTopicProducerConfig());
        taskTopicPartitions = config.taskTopicPartitions();
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsetsFromZk(taskTopicPartitions);
        //  Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());
        log.info("Starting with {} previously uncommitted partitions.", topicPartitionOffsets.entrySet().stream()
            .filter(x -> x.getValue() == 0L).count());
        log.trace("Seeking offsets: {}", topicPartitionOffsets);
        topicPartitionOffsets.forEach(consumer::seek);
        log.info("{} replicating {} topic-partitions {}->{}: {}.", Thread.currentThread().getName(),
            taskTopicPartitions.size(), sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions);

        // 注册当前task至消费组ids&owners下
        registerConsumerInZK();

    }

    @Override
    public void commit() {
        // Publish any offset syncs that we've queued up, but have not yet been able to publish
        // (likely because we previously reached our limit for number of outstanding syncs)
        firePendingOffsetSyncs();

        if (taskTopicPartitions == null) {
            return;
        }

        // 保存消费组offset至zk，兼容现有zk消费组offset同步机制
        taskTopicPartitions.forEach(topicPartition -> {
            Long upstreamOffset = loadOffset(topicPartition);
            if (upstreamOffset != null && upstreamOffset.longValue() > 0) {
                byte[] data = String.valueOf(upstreamOffset).getBytes(StandardCharsets.UTF_8);
                String path = getConsumerPath(topicPartition, sfMm2ConsumerGroupId);
                try {
                    sourceZkClient.create().orSetData().creatingParentsIfNeeded().forPath(path, data);
                } catch (Exception e) {
                    log.error("提交消费组offset报错，消费组{}，主题分区{}-{}，位点offset {}", sfMm2ConsumerGroupId,
                            topicPartition.topic(),
                            topicPartition.partition(), upstreamOffset, e);
                }
            }

        });
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        stopping = true;
        consumer.wakeup();
        try {
            consumerAccess.acquire();
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for access to consumer. Will try closing anyway."); 
        }
        Utils.closeQuietly(consumer, "source consumer");
        Utils.closeQuietly(offsetProducer, "offset producer");
        Utils.closeQuietly(metrics, "metrics");
        // Utils.closeQuietly(sourceZkClient, "source zk client");
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(), System.currentTimeMillis() - start);
    }
   
    @Override
    public String version() {
        return new MirrorSourceConnector().version();
    }

    @Override
    public List<SourceRecord> poll() {
        if (!consumerAccess.tryAcquire()) {
            return null;
        }
        if (stopping) {
            return null;
        }
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                // 是否需要同步该消息至下游集群
                boolean needReplicator = true;
                if (provenanceHeaderEnable) {
                    Iterable<Header> headers = record.headers().headers(REPLICATOR_ID_KEY);
                    for (Header header : headers) {
                        if (targetClusterAlias.equals(new String(header.value()))) {
                            needReplicator = false;
                            break;
                        }
                    }

                    if (needReplicator) {
                        // 是否要添加`__SF_REPLICATOR_ID` header
                        boolean needAddReplicatorHeader = true;
                        Header header = record.headers().lastHeader(REPLICATOR_ID_KEY);
                        if (header != null) {
                            String value = new String(header.value());
                            if (sourceClusterAlias.equals(value)) {
                                needAddReplicatorHeader = false;
                            }
                        }
                        if (needAddReplicatorHeader) {
                            record.headers()
                                    .add(REPLICATOR_ID_KEY, sourceClusterAlias.getBytes(StandardCharsets.UTF_8));
                        }
                    }
                }


                if (needReplicator) {
                    SourceRecord converted = convertRecord(record);
                    sourceRecords.add(converted);
                    TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                    metrics.recordAge(topicPartition, System.currentTimeMillis() - record.timestamp());
                    metrics.recordBytes(topicPartition, byteSize(record.value()));
                }

            }
            if (sourceRecords.isEmpty()) {
                // WorkerSourceTasks expects non-zero batch size
                return null;
            } else {
                log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
                return sourceRecords;
            }
        } catch (WakeupException e) {
            return null;
        } catch (KafkaException e) {
            log.warn("Failure during poll.", e);
            return null;
        } catch (Throwable e)  {
            log.error("Failure during poll.", e);
            // allow Connect to deal with the exception
            throw e;
        } finally {
            consumerAccess.release();
        }
    }
 
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (stopping) {
            return;
        }
        if (metadata == null) {
            log.debug("No RecordMetadata (source record was probably filtered out during transformation) -- can't sync offsets for {}.", record.topic());
            return;
        }
        if (!metadata.hasOffset()) {
            log.error("RecordMetadata has no offset -- can't sync offsets for {}.", record.topic());
            return;
        }
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        long latency = System.currentTimeMillis() - record.timestamp();
        metrics.countRecord(topicPartition);
        metrics.replicationLatency(topicPartition, latency);
        TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
        long upstreamOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
        long downstreamOffset = metadata.offset();
        maybeQueueOffsetSyncs(sourceTopicPartition, upstreamOffset, downstreamOffset);
        // We may be able to immediately publish an offset sync that we've queued up here
        firePendingOffsetSyncs();
    }

    // updates partition state and queues up OffsetSync if necessary
    private void maybeQueueOffsetSyncs(TopicPartition topicPartition, long upstreamOffset,
                                       long downstreamOffset) {
        PartitionState partitionState =
            partitionStates.computeIfAbsent(topicPartition, x -> new PartitionState(maxOffsetLag));
        if (partitionState.update(upstreamOffset, downstreamOffset)) {
            OffsetSync offsetSync = new OffsetSync(topicPartition, upstreamOffset, downstreamOffset);
            synchronized (this) {
                pendingOffsetSyncs.put(topicPartition, offsetSync);
            }
            partitionState.reset();
        }
    }

    private void firePendingOffsetSyncs() {
        while (true) {
            OffsetSync pendingOffsetSync;
            synchronized (this) {
                Iterator<OffsetSync> syncIterator = pendingOffsetSyncs.values().iterator();
                if (!syncIterator.hasNext()) {
                    // Nothing to sync
                    log.trace("No more pending offset syncs");
                    return;
                }
                pendingOffsetSync = syncIterator.next();
                if (!outstandingOffsetSyncs.tryAcquire()) {
                    // Too many outstanding syncs
                    log.trace("Too many in-flight offset syncs; will try to send remaining offset syncs later");
                    return;
                }
                syncIterator.remove();
            }
            // Publish offset sync outside of synchronized block; we may have to
            // wait for producer metadata to update before Producer::send returns
            sendOffsetSync(pendingOffsetSync);
            log.trace("Dispatched offset sync for {}", pendingOffsetSync.topicPartition());
        }
    }

    // sends OffsetSync record to internal offsets topic
    private void sendOffsetSync(OffsetSync offsetSync) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(offsetSyncsTopic, 0,
                offsetSync.recordKey(), offsetSync.recordValue());
        offsetProducer.send(record, (x, e) -> {
            if (e != null) {
                log.error("Failure sending offset sync.", e);
            } else {
                log.trace("Sync'd offsets for {}: {}=={}", offsetSync.topicPartition(),
                    offsetSync.upstreamOffset(), offsetSync.downstreamOffset());
            }
            outstandingOffsetSyncs.release();
        });
    }
 
    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, this::loadOffset));
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset = context.offsetStorageReader().offset(wrappedPartition);
        return MirrorUtils.unwrapOffset(wrappedOffset) + 1;
    }

    // 启动的时候从zk获取offset
    private Map<TopicPartition, Long> loadOffsetsFromZk(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, this::loadOffsetFromZk));
    }

    private Long loadOffsetFromZk(TopicPartition topicPartition) {
        Long rs = 0L;
        String path = getConsumerPath(topicPartition, sfMm2ConsumerGroupId);
        try {

            Stat stat = sourceZkClient.checkExists().forPath(path);
            if (stat == null) {
                sourceZkClient.create().creatingParentsIfNeeded().forPath(path, "0".getBytes(StandardCharsets.UTF_8));
            } else {
                byte[] data = sourceZkClient.getData().forPath(path);
                if (data != null) {
                    rs = Long.parseLong(new String(data));
                }
            }

        } catch (Exception e) {
            log.warn("启动时获取offset报错，ZK路径{}", path, e);
        }

        return Math.max(0L, rs);
    }

    // 注册当前task至消费组ids owners下
    private void registerConsumerInZK() {
        //{
        //  "version": 1,
        //  "subscription": {
        //    "EOS_FOP_QMS_SX_EXCEPWAYBILLINFO": 1
        //  },
        //  "pattern": "static",
        //  "timestamp": "1644262684333"
        //}
        Map<String, Integer> topicCount = Maps.newHashMap();
        taskTopicPartitions.forEach(tp -> topicCount.putIfAbsent(tp.topic(), 1));

        Map<String, Object> consumerData = Maps.newHashMap();
        consumerData.put("version", 1);
        consumerData.put("pattern", "static");
        consumerData.put("timestamp", String.valueOf(System.currentTimeMillis()));
        consumerData.put("subscription", topicCount);

        try {
            // ids
            String idsPath = getConsumerGroupIdsPath(sfMm2ConsumerGroupId);
            String clientId = InetAddress.getLocalHost().getHostAddress() + "-" + getClientIdMethod.invoke(consumer);
            sourceZkClient.create().orSetData().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                    .forPath(idsPath + "/" + clientId, JSON_MAPPER.writeValueAsBytes(consumerData));

            // owners
            taskTopicPartitions.forEach(tp -> {
                String path = getConsumerOwnersPath(sfMm2ConsumerGroupId, tp);
                try {
                    sourceZkClient.create().orSetData().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                            .forPath(path, clientId.getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    log.error("设置消费者owner报错", e);
                }
            });

        } catch (KeeperException.NodeExistsException e) {
            // ignore
        } catch (Exception e) {
            log.error("注册当前消费组id报错", e);
            stop();
            Exit.exit(6);
        }
    }

    private void checkBidirectionSync(String targetClusterZkServers, String groupId) {
        log.info("SF Kafka MirrorMaker2 循环同步探测中 ...");
        RetryPolicy retryPolicy = new BoundedExponentialBackoffRetry(100, 10000, 10);
        CuratorFramework targetZkClient = CuratorFrameworkFactory.newClient(targetClusterZkServers, retryPolicy);
        targetZkClient.start();

        String path = "/consumers/" + groupId + "/ids";
        try {
            Stat stat = targetZkClient.checkExists().forPath(path);
            if (stat != null) {
                List<String> consumerIds = targetZkClient.getChildren()
                        .forPath(path);
                targetZkClient.close();
                if (consumerIds != null && consumerIds.size() > 0) {

                    String msg = String.format("循环同步了！请确认下游集群[%s]同步消费组[%s]是否还在运行中？", targetClusterZkServers,
                            groupId);
                    System.err.println(msg);
                    Exit.exit(4);
                }
            }
        } catch (Exception e) {
            log.error("循环同步探测报错", e);
            Exit.exit(5);
        }

        log.info("SF Kafka MirrorMaker2 循环同步检测通过 ...");
    }

    // visible for testing 
    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers = convertHeaders(record);
        return new SourceRecord(
                MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
                MirrorUtils.wrapOffset(record.offset()),
                targetTopic, record.partition(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
                Schema.BYTES_SCHEMA, record.value(),
                record.timestamp(), headers);
    }

    private Headers convertHeaders(ConsumerRecord<byte[], byte[]> record) {
        ConnectHeaders headers = new ConnectHeaders();
        for (Header header : record.headers()) {
            headers.addBytes(header.key(), header.value());
        }
        return headers;
    }

    private String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);
    }

    private static int byteSize(byte[] bytes) {
        if (bytes == null) {
            return 0;
        } else {
            return bytes.length;
        }
    }

    static class PartitionState {
        long previousUpstreamOffset = -1L;
        long previousDownstreamOffset = -1L;
        long lastSyncDownstreamOffset = -1L;
        long maxOffsetLag;
        boolean shouldSyncOffsets;

        PartitionState(long maxOffsetLag) {
            this.maxOffsetLag = maxOffsetLag;
        }

        // true if we should emit an offset sync
        boolean update(long upstreamOffset, long downstreamOffset) {
            // Emit an offset sync if any of the following conditions are true
            boolean noPreviousSyncThisLifetime = lastSyncDownstreamOffset == -1L;
            // the OffsetSync::translateDownstream method will translate this offset 1 past the last sync, so add 1.
            // TODO: share common implementation to enforce this relationship
            boolean translatedOffsetTooStale = downstreamOffset - (lastSyncDownstreamOffset + 1) >= maxOffsetLag;
            boolean skippedUpstreamRecord = upstreamOffset - previousUpstreamOffset != 1L;
            boolean truncatedDownstreamTopic = downstreamOffset < previousDownstreamOffset;
            if (noPreviousSyncThisLifetime || translatedOffsetTooStale || skippedUpstreamRecord || truncatedDownstreamTopic) {
                lastSyncDownstreamOffset = downstreamOffset;
                shouldSyncOffsets = true;
            }
            previousUpstreamOffset = upstreamOffset;
            previousDownstreamOffset = downstreamOffset;
            return shouldSyncOffsets;
        }

        void reset() {
            shouldSyncOffsets = false;
        }
    }
}
