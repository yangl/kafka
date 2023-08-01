package org.apache.kafka.connect.mirror;

import static org.apache.kafka.connect.mirror.MirrorMakerConfig.SOURCE_CLUSTER_PREFIX;
import static org.apache.kafka.connect.mirror.MirrorMakerConfig.TARGET_CLUSTER_PREFIX;
import org.apache.kafka.common.TopicPartition;

/**
 * @author YANGLiiN
 */
public class SFMirrorMakerConstants {

    public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";
    public static final String SOURCE_CLUSTER_ZOOKEEPER_SERVERS = SOURCE_CLUSTER_PREFIX + ZOOKEEPER_SERVERS;
    public static final String TARGET_CLUSTER_ZOOKEEPER_SERVERS = TARGET_CLUSTER_PREFIX + ZOOKEEPER_SERVERS;

    public static final String CONSUMERS_PATH = "/consumers";
    public static final String CONSUMER_IDS_PATH_FORMAT = "/consumers/%s/ids";
    public static final String CONSUMER_OFFSETS_PATH_FORMAT = "/consumers/%s/offsets";
    public static final String CONSUMER_OWNERS_PATH_FORMAT = "/consumers/%s/owners/%s/%d";
    public static final String CONSUMER_PATH_FORMAT = "/consumers/%s/offsets/%s/%d";

    public static final String MM2_CONSUMER_IDS_PATH_FORMAT = "/mm2-sync/data/%s/ids";

    public static final String REPLICATOR_ID_KEY = "__SF_REPLICATOR_ID";

    public static final String PROVENANCE_HEADER_ENABLE_KEY = "provenance.header.enable";
    public static final String MM2_CONSUMER_GROUP_ID_KEY = "SF_MM2_CONSUMER_GROUP_ID";


    // 获取消费组路径
    public static final String getConsumerPath(TopicPartition tp, String groupId) {
        return String.format(CONSUMER_PATH_FORMAT, groupId, tp.topic(), tp.partition());
    }

    // 获取消费组ids路径
    public static final String getConsumerGroupIdsPath(String groupId) {
        return String.format(CONSUMER_IDS_PATH_FORMAT, groupId);
    }

    // 获取消费组owners路径
    public static final String getConsumerOwnersPath(String groupId, TopicPartition tp) {
        return String.format(CONSUMER_OWNERS_PATH_FORMAT, groupId, tp.topic(), tp.partition());
    }


    // 获取mm2消费组路径
    public static final String getMM2ConsumerGroupIdsPath(String groupId) {
        return String.format(MM2_CONSUMER_IDS_PATH_FORMAT, groupId);
    }
}
