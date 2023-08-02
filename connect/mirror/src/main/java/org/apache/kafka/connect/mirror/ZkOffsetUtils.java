package org.apache.kafka.connect.mirror;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.OptionalLong;

import static org.apache.kafka.connect.mirror.SFMirrorMakerConstants.*;

public class ZkOffsetUtils {
    private static final Logger log = LoggerFactory.getLogger(ZkOffsetUtils.class);


    // 同步标准Kafka消费组的offset
    protected static final void syncOffsets(CuratorFramework szk, CuratorFramework tzk, OffsetSyncStore offsetSyncStore) {

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
                            String path = String.format(CONSUMER_PATH_FORMAT, group, topic, p);
                            setData(tzk, path, String.valueOf(down.getAsLong()).getBytes());
                        }
                    }

                });
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
            cli.create().orSetData().creatingParentContainersIfNeeded()
                    .forPath(path, data);
        } catch (Exception e) {
            log.error("设置节点[{}]数据报错", path, e);
        }
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
