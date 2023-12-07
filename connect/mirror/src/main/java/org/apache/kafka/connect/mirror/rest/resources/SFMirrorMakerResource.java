package org.apache.kafka.connect.mirror.rest.resources;

import com.google.common.collect.Maps;
import io.swagger.v3.oas.annotations.Operation;
import org.apache.kafka.connect.runtime.rest.resources.ConnectResource;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Map;

import static org.apache.kafka.connect.mirror.SFMirrorMakerConstants.LASTEST_SYNC_GROUPOFFSETS_TOPIC;
import static org.apache.kafka.connect.mirror.SFMirrorMakerConstants.LASTEST_SYNC_GROUPOFFSETS_ZK;

@Path("/sf/customer")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class SFMirrorMakerResource implements ConnectResource {

    @GET
    @Path("/latest-sync-group-offsets-time")
    @Operation(summary = "Get consumer group latest sync time zk-topic")
    public Map<String, Long> latestSyncGroupOffsetsTime() {
        Map<String, Long> data = Maps.newHashMap();
        // 基于ZK的消费组位点
        data.put("zk", LASTEST_SYNC_GROUPOFFSETS_ZK);
        // 基于内部主题的消费组位点
        data.put("topic", LASTEST_SYNC_GROUPOFFSETS_TOPIC);
        return data;
    }

    @Override
    public void requestTimeout(long requestTimeoutMs) {
        // No-op
    }
}
