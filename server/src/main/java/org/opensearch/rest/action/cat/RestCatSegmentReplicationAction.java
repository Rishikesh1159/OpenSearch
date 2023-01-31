/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.cat;

import org.apache.lucene.util.CollectionUtil;
import org.opensearch.action.admin.indices.segment_replication.SegmentReplicationRequest;
import org.opensearch.action.admin.indices.segment_replication.SegmentReplicationResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.common.Table;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentOpenSearchExtension;
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.HashSet;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * RestCatSegmentReplicationAction provides information about the status of replica's segment replication event
 * in a string format, designed to be used at the command line. An Index can
 * be specified to limit output to a particular index or indices.
 *
 * @opensearch.api
 */
public class RestCatSegmentReplicationAction extends AbstractCatAction {
    @Override
    public List<RestHandler.Route> routes() {
        return unmodifiableList(
            asList(new RestHandler.Route(GET, "/_cat/segment_replication"), new RestHandler.Route(GET, "/_cat/segment_replication/{index}"))
        );
    }

    @Override
    public String getName() {
        return "cat_segment_replication_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/segment_replication\n");
        sb.append("/_cat/segment_replication/{index}\n");
    }

    @Override
    public BaseRestHandler.RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final SegmentReplicationRequest segmentReplicationRequest = new SegmentReplicationRequest(
            Strings.splitStringByCommaToArray(request.param("index"))
        );
        segmentReplicationRequest.timeout(request.param("timeout"));
        segmentReplicationRequest.detailed(request.paramAsBoolean("detailed", false));
        segmentReplicationRequest.shards(Strings.splitStringByCommaToArray(request.param("shards")));
        segmentReplicationRequest.activeOnly(request.paramAsBoolean("active_only", false));
        segmentReplicationRequest.indicesOptions(IndicesOptions.fromRequest(request, segmentReplicationRequest.indicesOptions()));

        return channel -> client.admin()
            .indices()
            .segment_replication(segmentReplicationRequest, new RestResponseListener<SegmentReplicationResponse>(channel) {
                @Override
                public RestResponse buildResponse(final SegmentReplicationResponse response) throws Exception {
                    return RestTable.buildResponse(buildSegmentReplicationTable(request, response), channel);
                }
            });
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {

        boolean detailed = false;
        if (request != null) {
            detailed = Boolean.parseBoolean(request.param("detailed"));
        }

        Table t = new Table();
        t.startHeaders()
            .addCell("index", "alias:i,idx;desc:index name")
            .addCell("shardId", "desc: shard Id")
            .addCell("start_time", "default:false;alias:start;desc:segment replication start time")
            .addCell("start_time_millis", "default:false;alias:start_millis;desc:segment replication start time in epoch milliseconds")
            .addCell("stop_time", "default:false;alias:stop;desc:segment replication stop time")
            .addCell("stop_time_millis", "default:false;alias:stop_millis;desc:segment replication stop time in epoch milliseconds")
            .addCell("time", "alias:t,ti;desc:segment replication time")
            .addCell("stage", "alias:st;desc:segment replication stage")
            .addCell("source_host", "alias:shost;desc:source host")
            .addCell("source_node", "alias:snode;desc:source node name")
            .addCell("target_host", "alias:thost;desc:target host")
            .addCell("target_node", "alias:tnode;desc:target node name")
            .addCell("files_fetched", "alias:ff;desc:files fetched")
            .addCell("files_percent", "alias:fp;desc:percent of files fetched")
            .addCell("bytes_fetched", "alias:bf;desc:bytes fetched")
            .addCell("bytes_percent", "alias:bp;desc:percent of bytes fetched");
        if (detailed) {
            t.addCell("files", "alias:f;desc:number of files to fetch")
                .addCell("files_total", "alias:tf;desc:total number of files")
                .addCell("bytes", "alias:b;desc:number of bytes to fetch")
                .addCell("bytes_total", "alias:tb;desc:total number of bytes")
                .addCell("replication_id", "desc: replication Id")
                .addCell("replicating_stage_time_taken", "alias:rstt;desc:time taken in replicating stage")
                .addCell("get_checkpoint_info_stage_time_taken", "alias:gcistt;desc:time taken in get checkpoint info stage")
                .addCell("file_diff_stage_time_taken", "alias:fdstt;desc:time taken in file diff stage")
                .addCell("get_files_stage_time_taken", "alias:gfstt;desc:time taken in get files stage")
                .addCell("finalize_replication_stage_time_taken", "alias:frstt;desc:time taken in finalize replication stage");
        }
        t.endHeaders();
        return t;
    }

    protected Table buildCustomResponseTable(String reason) {
        Table t = new Table();
        t.startHeaders().addCell("Reason", "desc: Reason for API response").endHeaders();
        t.startRow();
        t.addCell(reason);
        t.endRow();
        return t;
    }

    /**
     * buildSegmentReplicationTable will build a table of SegmentReplication information suitable
     * for displaying at the command line.
     *
     * @param request  A Rest request
     * @param response A SegmentReplication status response
     * @return A table containing index, shardId, node, target size, recovered size and percentage for each fetching replica
     */
    public Table buildSegmentReplicationTable(RestRequest request, SegmentReplicationResponse response) {
        String[] indexNames = new String[0];
        boolean detailed = false;
        String[] shards = new String[0];
        Set<String> set = new HashSet<>();
        if (request != null) {
            indexNames = Strings.splitStringByCommaToArray(request.param("index"));
            detailed = Boolean.parseBoolean(request.param("detailed"));
            shards = Strings.splitStringByCommaToArray(request.param("shards"));
        }
        if (request != null && indexNames.length > 0) {
            for (String index : indexNames) {
                if (!response.shardSegmentReplicationStates().containsKey(index)) {
                    String reason = "error:" + "{Segment Replication is not enabled on index: " + index + " }";
                    return buildCustomResponseTable(reason);
                }
            }

        }
        if (shards.length > 0) {
            for (String shard : shards) {
                set.add(shard);
            }
        }
        Table t = getTableWithHeader(request);

        for (String index : response.shardSegmentReplicationStates().keySet()) {

            List<SegmentReplicationState> shardSegmentReplicationStates = response.shardSegmentReplicationStates().get(index);
            if (shardSegmentReplicationStates.size() == 0) {
                continue;
            }

            // Sort ascending by shard id for readability
            CollectionUtil.introSort(shardSegmentReplicationStates, new Comparator<SegmentReplicationState>() {
                @Override
                public int compare(SegmentReplicationState o1, SegmentReplicationState o2) {
                    int id1 = o1.getShardRouting().shardId().id();
                    int id2 = o2.getShardRouting().shardId().id();
                    if (id1 < id2) {
                        return -1;
                    } else if (id1 > id2) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            for (SegmentReplicationState state : shardSegmentReplicationStates) {
                int shardId = state.getShardRouting().shardId().id();
                if (shards.length > 0 && !set.contains(Integer.toString(shardId))) {
                    continue;
                }
                t.startRow();
                t.addCell(index);
                t.addCell(state.getShardRouting().shardId().id());
                t.addCell(XContentOpenSearchExtension.DEFAULT_DATE_PRINTER.print(state.getTimer().startTime()));
                t.addCell(state.getTimer().startTime());
                t.addCell(XContentOpenSearchExtension.DEFAULT_DATE_PRINTER.print(state.getTimer().stopTime()));
                t.addCell(state.getTimer().stopTime());
                t.addCell(new TimeValue(state.getTimer().time()));
                t.addCell(state.getStage().toString().toLowerCase(Locale.ROOT));
                t.addCell(state.getSourceNode().getHostName());
                t.addCell(state.getSourceNode().getName());
                t.addCell(state.getTargetNode().getHostName());
                t.addCell(state.getTargetNode().getName());
                t.addCell(state.getIndex().recoveredFileCount());
                t.addCell(String.format(Locale.ROOT, "%1.1f%%", state.getIndex().recoveredFilesPercent()));
                t.addCell(state.getIndex().recoveredBytes());
                t.addCell(String.format(Locale.ROOT, "%1.1f%%", state.getIndex().recoveredBytesPercent()));
                if (detailed) {
                    t.addCell(state.getIndex().totalRecoverFiles());
                    t.addCell(state.getIndex().totalFileCount());
                    t.addCell(state.getIndex().totalRecoverBytes());
                    t.addCell(state.getIndex().totalBytes());
                    t.addCell(state.getReplicationId());
                    t.addCell(new TimeValue(state.getReplicating().time()));
                    t.addCell(new TimeValue(state.getGetCheckpointInfo().time()));
                    t.addCell(new TimeValue(state.getFileDiff().time()));
                    t.addCell(new TimeValue(state.getGetFile().time()));
                    t.addCell(new TimeValue(state.getFinalizeReplication().time()));
                }
                t.endRow();
            }
        }

        return t;
    }
}
