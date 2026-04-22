#include <gtest/gtest.h>
#include "../../src/meta/meta_coordinator.h"
#include "../../src/common/config.h"

static nimbus::NodeConfig make_cfg() {
    nimbus::NodeConfig cfg;
    cfg.node_id  = "meta0";
    cfg.role     = "leader";
    cfg.address  = "127.0.0.1:9100";
    cfg.data_dir = "/tmp";
    cfg.mode     = "adaptive";
    return cfg;
}

static nimbus::NodeEntry make_node(const std::string& id,
                                    float load = 0.2f,
                                    uint64_t free_bytes = 1ULL << 30,
                                    uint64_t total_bytes = 4ULL << 30) {
    nimbus::NodeEntry n;
    n.node_id     = id;
    n.address     = "127.0.0.1:9200";
    n.load_fraction = load;
    n.free_bytes  = free_bytes;
    n.total_bytes = total_bytes;
    n.failure_rate_7d = 0.0f;
    n.last_seen_ms = 1000;
    n.alive = true;
    return n;
}

TEST(CoordinatorTest, PutChunkStoresEntry) {
    nimbus::MetaCoordinator coord(make_cfg());
    coord.apply_put_chunk("chunk1", 4096, 3, {"n1","n2","n3"}, 1);
    auto* e = coord.get_chunk("chunk1");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->chunk_id, "chunk1");
    EXPECT_EQ(e->version, 1u);
    EXPECT_EQ(e->replication_factor, 3);
    EXPECT_EQ(e->replica_set.size(), 3u);
    EXPECT_EQ(e->config_state, nimbus::ChunkConfigState::STABLE);
}

TEST(CoordinatorTest, PutChunkIncrementsVersion) {
    nimbus::MetaCoordinator coord(make_cfg());
    coord.apply_put_chunk("chunk1", 4096, 3, {"n1","n2","n3"}, 1);
    coord.apply_put_chunk("chunk1", 4096, 3, {"n1","n2","n3"}, 2);
    auto* e = coord.get_chunk("chunk1");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->version, 2u);
}

TEST(CoordinatorTest, DeleteChunkRemovesEntry) {
    nimbus::MetaCoordinator coord(make_cfg());
    coord.apply_put_chunk("chunk1", 4096, 3, {"n1","n2","n3"}, 1);
    coord.apply_delete_chunk("chunk1");
    EXPECT_EQ(coord.get_chunk("chunk1"), nullptr);
}

TEST(CoordinatorTest, ReconfigStartSetsTransitioning) {
    nimbus::MetaCoordinator coord(make_cfg());
    coord.apply_put_chunk("chunk1", 4096, 3, {"n1","n2","n3"}, 1);
    coord.apply_reconfig_start("chunk1", {"n1","n2","n3"}, {"n4","n5","n6","n7","n8"});
    auto* e = coord.get_chunk("chunk1");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->config_state, nimbus::ChunkConfigState::TRANSITIONING);
    EXPECT_EQ(e->old_replica_set.size(), 3u);
    EXPECT_EQ(e->replica_set.size(), 5u);
}

TEST(CoordinatorTest, ReconfigCommitSetsStable) {
    nimbus::MetaCoordinator coord(make_cfg());
    coord.apply_put_chunk("chunk1", 4096, 3, {"n1","n2","n3"}, 1);
    coord.apply_reconfig_start("chunk1", {"n1","n2","n3"}, {"n4","n5"});
    coord.apply_reconfig_commit("chunk1", {"n4","n5"});
    auto* e = coord.get_chunk("chunk1");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->config_state, nimbus::ChunkConfigState::STABLE);
    EXPECT_TRUE(e->old_replica_set.empty());
    EXPECT_EQ(e->replica_set.size(), 2u);
}

TEST(CoordinatorTest, HeartbeatRegistersNode) {
    nimbus::MetaCoordinator coord(make_cfg());
    coord.apply_heartbeat("n1", "127.0.0.1:9200", 0.3f, 1<<30, 4<<30, 0.0f);
    auto nodes = coord.get_alive_nodes();
    ASSERT_EQ(nodes.size(), 1u);
    EXPECT_EQ(nodes[0].node_id, "n1");
}

TEST(CoordinatorTest, StaleNodeEviction) {
    nimbus::MetaCoordinator coord(make_cfg());
    coord.apply_heartbeat("n1", "127.0.0.1:9200", 0.3f, 1<<30, 4<<30, 0.0f);
    // Evict with a very large now_ms to simulate timeout.
    coord.evict_stale_nodes(999999999ULL, 3000);
    auto nodes = coord.get_alive_nodes();
    EXPECT_TRUE(nodes.empty());
}

TEST(CoordinatorTest, NextVersionMonotonicallyIncreases) {
    nimbus::MetaCoordinator coord(make_cfg());
    coord.apply_put_chunk("c1", 4096, 3, {"n1","n2","n3"}, 5);
    EXPECT_EQ(coord.next_version("c1"), 6u);
    EXPECT_EQ(coord.next_version("new_chunk"), 1u);
}
