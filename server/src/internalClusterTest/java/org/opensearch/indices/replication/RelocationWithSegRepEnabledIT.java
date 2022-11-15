/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.index.IndexFileNames;
import org.junit.BeforeClass;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.FileChunkRequest;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.MockIndexEventListener;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.test.transport.StubbableTransport;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.in;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.*;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RelocationWithSegRepEnabledIT extends OpenSearchIntegTestCase {

    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(5, TimeUnit.MINUTES);

    @BeforeClass
    public static void assumeFeatureFlag() {
//        assumeTrue("Segment replication Feature flag is enabled", Boolean.parseBoolean(System.getProperty(FeatureFlags.REPLICATION_TYPE)));
        assumeTrue("Segment replication Feature flag is enabled", true);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, MockTransportService.TestPlugin.class, MockIndexEventListener.TestPlugin.class);
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        super.beforeIndexDeletion();
        assertActiveCopiesEstablishedPeerRecoveryRetentionLeases();
        internalCluster().assertSeqNos();
        internalCluster().assertSameDocIdsOnShards();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            // sync global checkpoint quickly so we can verify seq_no_stats aligned between all copies after tests.
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }


    /**
     * This Integration Relocates a primary shard to another node. Before Relocation and after relocation we index single document. We don't perform any flush
     * before relocation is done.
     * This test will pass if we perform flush before relocation.
     */
    public void testSimplePrimaryRelocationWithoutFlushBeforeRelocation() throws Exception{
        logger.info("--> starting [node1] ...");
        final String node_1 = internalCluster().startNode();

        logger.info("--> creating test index ...");
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 1)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.number_of_replicas", 1)).get();

        final String node_2 = internalCluster().startNode();

        ensureGreen("test");

        logger.info("--> index 1 doc");
        client().prepareIndex("test").setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        waitForReplicaUpdate();
        logger.info("--> verifying count");
        assertThat(client(node_1).prepareSearch("test").setSize(0).setPreference("_local").execute().actionGet().getHits().getTotalHits().value, equalTo(1L));
        assertThat(client(node_2).prepareSearch("test").setSize(0).setPreference("_local").execute().actionGet().getHits().getTotalHits().value, equalTo(1L));


        // If we do a flush before relocation, this test will pass.
//        flush("test");


        logger.info("--> start another node");
        final String node_3 = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("3")
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> relocate the shard from node1 to node3");

        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, node_1, node_3)).execute().actionGet();
        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> Done relocating the shard from node1 to node3");


        client().prepareIndex("test").setId("2").setSource("bar", "baz").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        waitForReplicaUpdate();
        logger.info("--> verifying count again...");
            client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client().prepareSearch("test").setSize(0).execute().actionGet().getHits().getTotalHits().value, equalTo(2L));
    }

    /**
     * This Integration Relocates a primary shard to another node for 5 times. While the relocation of primary is in progress,
     * we randomly index few documents and perform refresh.
     */
    public void testPrimaryRelocationWhileRefreshing() throws Exception {
//        int numberOfRelocations = scaledRandomIntBetween(1, rarely() ? 10 : 4);
//        int numberOfReplicas = randomBoolean() ? 0 : 1;
//        int numberOfNodes = numberOfReplicas == 0 ? 2 : 3;

        int numberOfRelocations = 5;
        int numberOfReplicas = 1;
        int numberOfNodes = 3;

        logger.info(
            "testRelocationWhileIndexingRandom(numRelocations={}, numberOfReplicas={}, numberOfNodes={})",
            numberOfRelocations,
            numberOfReplicas,
            numberOfNodes
        );

        String[] nodes = new String[numberOfNodes];
        logger.info("--> starting [node_0] ...");
        nodes[0] = internalCluster().startNode();

        logger.info("--> creating test index ...");
        prepareCreate(
            "test",
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", numberOfReplicas)
                .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                // we want to control refreshes
                .put("index.refresh_interval", -1)
        ).get();

        for (int i = 1; i < numberOfNodes; i++) {
            logger.info("--> starting [node_{}] ...", i);
            nodes[i] = internalCluster().startNode();
            if (i != numberOfNodes - 1) {
                ClusterHealthResponse healthResponse = client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForNodes(Integer.toString(i + 1))
                    .setWaitForGreenStatus()
                    .execute()
                    .actionGet();
                assertThat(healthResponse.isTimedOut(), equalTo(false));
            }
        }

        final Semaphore startedShards = new Semaphore(0);
        final IndexEventListener listener = new IndexEventListener() {
            @Override
            public void indexShardStateChanged(
                IndexShard indexShard,
                @Nullable IndexShardState previousState,
                IndexShardState currentState,
                @Nullable String reason
            ) {
                if (currentState == IndexShardState.STARTED) {
                    startedShards.release();
                }
            }
        };
        for (MockIndexEventListener.TestEventListener eventListener : internalCluster().getInstances(
            MockIndexEventListener.TestEventListener.class
        )) {
            eventListener.setNewDelegate(listener);
        }

        ShardRouting node0ShardRouting = getShardRoutingForNodeName(nodes[0]);
        ShardRouting node1ShardRouting = getShardRoutingForNodeName(nodes[1]);
        ShardRouting node2ShardRouting = getShardRoutingForNodeName(nodes[2]);

        logger.info("Node 0 shard routing before relocation: "+node0ShardRouting);
        logger.info("Node 1 shard routing before relocation: "+node1ShardRouting);
        logger.info("Node 2 shard routing before relocation: "+node2ShardRouting);

        logger.info("--> starting relocations...");
        int nodeShiftBased = numberOfReplicas; // if we have replicas shift those
        for (int i = 0; i < numberOfRelocations; i++) {
//            int fromNode = (i % 2);
//            int toNode = fromNode == 0 ? 1 : 0;
//            fromNode += nodeShiftBased;
//            toNode += nodeShiftBased;

            int fromNode;
            int toNode;
            if(i % 2 == 0 ){
                fromNode = 0;
                toNode = 2;
            }
            else{
                fromNode = 2;
                toNode =0;
            }

            List<IndexRequestBuilder> builders1 = new ArrayList<>();
            for (int numDocs = randomIntBetween(10, 30); numDocs > 0; numDocs--) {
                builders1.add(client().prepareIndex("test").setSource("{}", XContentType.JSON));
            }

            List<IndexRequestBuilder> builders2 = new ArrayList<>();
            for (int numDocs = randomIntBetween(10, 30); numDocs > 0; numDocs--) {
                builders2.add(client().prepareIndex("test").setSource("{}", XContentType.JSON));
            }

            logger.info("--> START relocate the shard from {} to {}", nodes[fromNode], nodes[toNode]);

            client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, nodes[fromNode], nodes[toNode])).get();

            logger.debug("--> index [{}] documents", builders1.size());
            indexRandom(false, true, builders1);
            // wait for shard to start
            startedShards.acquire(1);

            logger.debug("--> index [{}] documents", builders2.size());
            indexRandom(true, true, builders2);

            // verify cluster was finished.
            assertFalse(
                client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForNoRelocatingShards(true)
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout("30s")
                    .get()
                    .isTimedOut()
            );
            logger.info("--> DONE relocate the shard from {} to {}", fromNode, toNode);

            ShardRouting afterNode0ShardRouting = getShardRoutingForNodeName(nodes[0]);
            ShardRouting afterNode1ShardRouting = getShardRoutingForNodeName(nodes[1]);
            ShardRouting afterNode2ShardRouting = getShardRoutingForNodeName(nodes[2]);

            logger.info("Node 0 shard routing after relocation: "+afterNode0ShardRouting);
            logger.info("Node 1 shard routing after relocation: "+afterNode1ShardRouting);
            logger.info("Node 2 shard routing after relocation: "+afterNode2ShardRouting);

            waitForReplicaUpdate();

            logger.debug("--> verifying all searches return the same number of docs");
            assertBusy(()-> {
                long expectedCount = -1;
                for (String node : nodes) {
                    SearchResponse response = client(node).prepareSearch("test").setSize(0).setPreference("_local").get();
                    assertNoFailures(response);
                    if (expectedCount < 0) {
                        expectedCount = response.getHits().getTotalHits().value;
                    } else {
                        assertEquals(expectedCount, response.getHits().getTotalHits().value);
                    }
                }
            });

        }

    }

    private void waitForReplicaUpdate() throws Exception {
        // wait until the replica has the latest segment generation.
        assertBusy(() -> {
            final IndicesSegmentResponse indicesSegmentResponse = client().admin()
                .indices()
                .segments(new IndicesSegmentsRequest())
                .actionGet();
            List<ShardSegments[]> segmentsByIndex = getShardSegments(indicesSegmentResponse);
            for (ShardSegments[] replicationGroupSegments : segmentsByIndex) {
                final Map<Boolean, List<ShardSegments>> segmentListMap = segmentsByShardType(replicationGroupSegments);
                final List<ShardSegments> primaryShardSegmentsList = segmentListMap.get(true);
                final List<ShardSegments> replicaShardSegments = segmentListMap.get(false);
                // if we don't have any segments yet, proceed.
                final ShardSegments primaryShardSegments = primaryShardSegmentsList.stream().findFirst().get();
                logger.debug("Primary Segments: {}", primaryShardSegments.getSegments());
                if (primaryShardSegments.getSegments().isEmpty() == false) {
                    final Map<String, Segment> latestPrimarySegments = getLatestSegments(primaryShardSegments);
                    final Long latestPrimaryGen = latestPrimarySegments.values().stream().findFirst().map(Segment::getGeneration).get();
                    for (ShardSegments shardSegments : replicaShardSegments) {
                        logger.debug("Replica {} Segments: {}", shardSegments.getShardRouting(), shardSegments.getSegments());
                        final boolean isReplicaCaughtUpToPrimary = shardSegments.getSegments()
                            .stream()
                            .anyMatch(segment -> segment.getGeneration() == latestPrimaryGen);
                        assertTrue(isReplicaCaughtUpToPrimary);
                    }
                }
            }
        });
    }

    private IndexShard getIndexShard(String node) {
        final Index index = resolveIndex("test");
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.indexServiceSafe(index);
        final Optional<Integer> shardId = indexService.shardIds().stream().findFirst();
        return indexService.getShard(shardId.get());
    }

    private List<ShardSegments[]> getShardSegments(IndicesSegmentResponse indicesSegmentResponse) {
        return indicesSegmentResponse.getIndices()
            .values()
            .stream() // get list of IndexSegments
            .flatMap(is -> is.getShards().values().stream()) // Map to shard replication group
            .map(IndexShardSegments::getShards) // get list of segments across replication group
            .collect(Collectors.toList());
    }

    private Map<String, Segment> getLatestSegments(ShardSegments segments) {
        final Optional<Long> generation = segments.getSegments().stream().map(Segment::getGeneration).max(Long::compare);
        final Long latestPrimaryGen = generation.get();
        return segments.getSegments()
            .stream()
            .filter(s -> s.getGeneration() == latestPrimaryGen)
            .collect(Collectors.toMap(Segment::getName, Function.identity()));
    }

    private Map<Boolean, List<ShardSegments>> segmentsByShardType(ShardSegments[] replicationGroupSegments) {
        return Arrays.stream(replicationGroupSegments).collect(Collectors.groupingBy(s -> s.getShardRouting().primary()));
    }

    @Nullable
    private ShardRouting getShardRoutingForNodeName(String nodeName) {
        final ClusterState state = client(internalCluster().getClusterManagerName()).admin().cluster().prepareState().get().getState();
        for (IndexShardRoutingTable shardRoutingTable : state.routingTable().index("test")) {
            for (ShardRouting shardRouting : shardRoutingTable.activeShards()) {
                final String nodeId = shardRouting.currentNodeId();
                final DiscoveryNode discoveryNode = state.nodes().resolveNode(nodeId);
                if (discoveryNode.getName().equals(nodeName)) {
                    return shardRouting;
                }
            }
        }
        return null;
    }

    private void assertActiveCopiesEstablishedPeerRecoveryRetentionLeases() throws Exception {
        assertBusy(() -> {
            for (ObjectCursor<String> it : client().admin().cluster().prepareState().get().getState().metadata().indices().keys()) {
                Map<ShardId, List<ShardStats>> byShardId = Stream.of(client().admin().indices().prepareStats(it.value).get().getShards())
                    .collect(Collectors.groupingBy(l -> l.getShardRouting().shardId()));
                for (List<ShardStats> shardStats : byShardId.values()) {
                    Set<String> expectedLeaseIds = shardStats.stream()
                        .map(s -> ReplicationTracker.getPeerRecoveryRetentionLeaseId(s.getShardRouting()))
                        .collect(Collectors.toSet());
                    for (ShardStats shardStat : shardStats) {
                        Set<String> actualLeaseIds = shardStat.getRetentionLeaseStats()
                            .retentionLeases()
                            .leases()
                            .stream()
                            .map(RetentionLease::id)
                            .collect(Collectors.toSet());
                        assertThat(expectedLeaseIds, everyItem(in(actualLeaseIds)));
                    }
                }
            }
        });
    }

}
