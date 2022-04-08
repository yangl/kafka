package org.apache.kafka.connect.mirror;

import java.io.Serializable;
import java.util.List;

public class SFOffsetSaveRequest implements Serializable {
    String sourceBrokerList;
    String targetBrokerList;
    List<TopicPartitionOffsetPair> offsets;

    public SFOffsetSaveRequest(String sourceBrokerList, String targetBrokerList,
                               List<TopicPartitionOffsetPair> offsets) {
        this.sourceBrokerList = sourceBrokerList;
        this.targetBrokerList = targetBrokerList;
        this.offsets = offsets;
    }


    public String getSourceBrokerList() {
        return sourceBrokerList;
    }

    public void setSourceBrokerList(String sourceBrokerList) {
        this.sourceBrokerList = sourceBrokerList;
    }

    public String getTargetBrokerList() {
        return targetBrokerList;
    }

    public void setTargetBrokerList(String targetBrokerList) {
        this.targetBrokerList = targetBrokerList;
    }

    public List<TopicPartitionOffsetPair> getOffsets() {
        return offsets;
    }

    public void setOffsets(List<TopicPartitionOffsetPair> offsets) {
        this.offsets = offsets;
    }


    static class TopicPartitionOffsetPair {
        String topic;
        int partition;
        long sourceOffset;
        long targetOffset;

        public TopicPartitionOffsetPair(String topic, int partition, long sourceOffset, long targetOffset) {
            this.topic = topic;
            this.partition = partition;
            this.sourceOffset = sourceOffset;
            this.targetOffset = targetOffset;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public long getSourceOffset() {
            return sourceOffset;
        }

        public void setSourceOffset(long sourceOffset) {
            this.sourceOffset = sourceOffset;
        }

        public long getTargetOffset() {
            return targetOffset;
        }

        public void setTargetOffset(long targetOffset) {
            this.targetOffset = targetOffset;
        }
    }

}


