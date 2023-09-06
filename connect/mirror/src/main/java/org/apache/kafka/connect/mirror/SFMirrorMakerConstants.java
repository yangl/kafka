package org.apache.kafka.connect.mirror;

import static org.apache.kafka.connect.mirror.MirrorMakerConfig.SOURCE_CLUSTER_PREFIX;
import static org.apache.kafka.connect.mirror.MirrorMakerConfig.TARGET_CLUSTER_PREFIX;

import java.net.InetAddress;
import java.net.UnknownHostException;

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
    public static final String CONSUMER_TOPIC_PARTITION_PATH_FORMAT = "/consumers/%s/offsets/%s/%d";

    public static final String MM2_CONSUMER_IDS_PATH_FORMAT = "/mm2-sync/data/%s/ids";
    public static final String MM2_OFFSETS_IDS_PATH_FORMAT = "/mm2-sync/offsets/ids";
    public static final String MM2_OFFSETS_LATCH_PATH_FORMAT = "/mm2-sync/offsets/latch";

    public static final String MM2_CONSUMER_GROUP_ID_KEY = "mm2.consumer.group.id";

    public static final String MM2_OFFSET_ZK_ENABLED_KEY = "sync.group.offsets.zk.enabled";
    public static final String MM2_TOPIC_DATA_ENABLED_KEY = "sync.topic.data.enabled";


    public static final String PROVENANCE_HEADER_ENABLED_KEY = "provenance.header.enabled";
    public static final String REPLICATOR_ID_KEY = "__SF_REPLICATOR_ID";


    // 获取mm2消费组路径
    public static final String getMM2ConsumerGroupIdsPath(String groupId) {
        return String.format(MM2_CONSUMER_IDS_PATH_FORMAT, groupId);
    }

    // 获取本机IP
    public static final String getIp() {
        String ip = "";
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
        }

        return ip;
    }
}
