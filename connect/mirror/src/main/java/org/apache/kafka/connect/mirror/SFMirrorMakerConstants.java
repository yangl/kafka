package org.apache.kafka.connect.mirror;

/**
 * @author YANGLiiN
 */
public class SFMirrorMakerConstants {

    public static final String CONSUMER_ZK_PATH_FORMAT = "/consumers/%s/offsets/%s/%d";
    public static final String REPLICATOR_ID_KEY = "__SF_REPLICATOR_ID";
    public static final String REPLICATOR_ID_SPLIT_KEY = "@@->";
    public static final String MM2_CONSUMER_GROUP_ID_KEY = "SF_MM2_CONSUMER_GROUP_ID";

}
