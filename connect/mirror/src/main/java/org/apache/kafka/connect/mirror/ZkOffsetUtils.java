package org.apache.kafka.connect.mirror;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.apache.kafka.connect.mirror.SFMirrorMakerConstants.*;

public class ZkOffsetUtils {
    private static final Logger log = LoggerFactory.getLogger(ZkOffsetUtils.class);


    public static final String JSTORM_NAMESPACE = "/transactional";
    public static final Map<String, Object> NODE_BROKER_DATA = Maps.newHashMap();

    static {
        NODE_BROKER_DATA.put("host", "");
        NODE_BROKER_DATA.put("port", 9092);
    }


    // 同步标准Kafka消费组的offset
    protected static final void syncZkOffsets(CuratorFramework szk, CuratorFramework tzk, OffsetSyncStore offsetSyncStore) {


        List<String> groups = getZkConsumerGroups(szk, tzk);
        groups.parallelStream().forEach(group -> {

            String topicPath = String.format(CONSUMER_OFFSETS_PATH_FORMAT, group);
            getChildren(szk, topicPath).parallelStream().forEach(topic -> {

                String partitionPath = topicPath + "/" + topic;
                getChildren(szk, partitionPath).forEach(partition -> {
                    int p = Integer.parseInt(partition);
                    TopicPartition tp = new TopicPartition(topic, p);
                    String up = getData(szk, partitionPath + "/" + partition);
                    if (!Strings.isNullOrEmpty(up)) {
                        OptionalLong down = offsetSyncStore.translateDownstream(group, tp, Long.parseLong(up));

                        if (down.isPresent() && down.getAsLong() > 0L) {
                            String path = String.format(CONSUMER_TOPIC_PARTITION_PATH_FORMAT, group, topic, p);
                            setData(tzk, path, String.valueOf(down.getAsLong()).getBytes());
                        }
                    }

                });
            });
        });
    }


    // 同步Jstorm消费组的offset
    protected static final void syncJstormOffsets(CuratorFramework szk, CuratorFramework tzk, OffsetSyncStore offsetSyncStore) {

        // /transactional
        List<String> bs = getChildren(szk, "/");
        bs.forEach(s -> {

            String consumerPath = "/" + s;
            List<String> consumers = getChildren(szk, consumerPath);

            consumers.parallelStream().forEach(c -> {

                String[] cs = StringUtils.split(c, "~");
                if (cs != null && cs.length == 3) {
                    String group = cs[0];
                    // String jstormClusterId = cs[1];
                    String topic = cs[2];

                    String sourceConsumerTopicPath = "/" + s + "/" + c;
                    String targetConsumerTopicPath = sourceConsumerTopicPath;
                    List<String> partitions = getChildren(szk, sourceConsumerTopicPath);
                    partitions.forEach(p -> {

                        String ps = StringUtils.substringAfterLast(p, "_");
                        String sourcePartitionPath = sourceConsumerTopicPath + "/" + p;
                        String targetPartitionPath = targetConsumerTopicPath + "/" + p;
                        String js = getData(szk, sourcePartitionPath);

                        JSONObject jo = JSON.parseObject(js);

                        if (jo == null) {
                            return;
                        }

                        /**
                         *
                         * {
                         *     "topology": {
                         *         "id": "aa1336a3-bfd3-4fe9-aa45-e38d233d599a",
                         *         "name": "FVP_CALC_BASE_UAT"
                         *     },
                         *     "offset": 271378,
                         *     "partition": 1,
                         *     "broker": {
                         *         "host": "10.208.16.95",
                         *         "port": 9092
                         *     },
                         *     "topic": "FVP_BASE_DATAIN_ORIGINAL_BAR"
                         * }
                         *
                         */

                        Object topologyData = jo.get("topology");

                        long up = jo.getLongValue("offset");
                        if (up <= 0) {
                            return;
                        }
                        Integer pid = Integer.parseInt(ps);
                        TopicPartition tp = new TopicPartition(topic, pid);

                        // 获取源集群主题最新消费情况
                        OptionalLong down = offsetSyncStore.translateDownstream(group, tp, up);

                        if (down.isPresent() && down.getAsLong() > 0L) {
                            Map<String, Object> jstomOffset = Maps.newHashMap();
                            jstomOffset.put("broker", NODE_BROKER_DATA);
                            jstomOffset.put("topic", topic);
                            jstomOffset.put("partition", pid);
                            jstomOffset.put("offset", down.getAsLong());
                            jstomOffset.put("topology", topologyData);

                            String offsetJson = JSON.toJSONString(jstomOffset);
                            setData(tzk, targetPartitionPath, offsetJson.getBytes());
                        }
                    });

                }
            });
        });


    }

    // 获取子节点
    private static List<String> getChildren(CuratorFramework cli, String path) {
        List<String> rs = Lists.newArrayList();
        try {
            Stat stat = cli.checkExists().forPath(path);
            if (stat != null) {
                rs = cli.getChildren().forPath(path);
            }
        } catch (Exception e) {
            log.error("获取子节点[{}]报错", path, e);
        }

        return rs;
    }

    // 获取节点数据
    private static String getData(CuratorFramework cli, String path) {
        byte[] data = null;
        try {
            data = cli.getData().forPath(path);
        } catch (Exception e) {
            log.error("获取节点[{}]数据报错", path, e);
        }

        if (data != null) {
            return new String(data);
        }

        return "";
    }

    // 设置节点数据
    private static void setData(CuratorFramework cli, String path, byte[] data) {
        try {
            cli.create().orSetData().creatingParentsIfNeeded().forPath(path, data);
        } catch (Exception e) {
            log.error("设置节点[{}]数据报错", path, e);
        }
    }


    // 创建节点
    protected static final void create(CuratorFramework cli, String path) {
        if (!StringUtils.startsWith(path, "/")) {
            path = "/" + path;
        }
        try {
            cli.create().creatingParentsIfNeeded().forPath(path);
        } catch (Exception e) {
            log.error("创建节点[{}]是否存在报错", path, e);
        }
    }

    // 检测节点是否存在
    protected static final boolean exists(CuratorFramework cli, String path) {
        Stat stat = null;
        if (!StringUtils.startsWith(path, "/")) {
            path = "/" + path;
        }
        try {
            stat = cli.checkExists().forPath(path);
        } catch (Exception e) {
            log.error("检测节点[{}]是否存在报错", path, e);
        }

        return stat != null;
    }

    // 根据zk获取消费组列表(不排除mirrormaker同步组)
    private static List<String> getZkConsumerGroups(CuratorFramework szk, CuratorFramework tzk) {
        List<String> rs = Lists.newArrayList();

        List<String> source = Lists.newArrayList();
        try {
            source = szk.getChildren().forPath(CONSUMERS_PATH);
        } catch (Exception e) {
            log.error("根据zk获取消费组列表报错了", e);
        }

        // 排除下游存活的消费组
        source.forEach(s -> {
            List<String> ids = null;
            try {
                ids = tzk.getChildren().forPath(String.format(CONSUMER_IDS_PATH_FORMAT, s));
            } catch (Exception e) {
                // ignore
            }
            if (ids == null || ids.isEmpty()) {
                rs.add(s);
            }
        });

        return rs;
    }
}
