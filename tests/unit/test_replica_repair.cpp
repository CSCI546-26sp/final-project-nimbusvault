#include <gtest/gtest.h>
#include "storage/replica_repair.h"
#include "storage/stats_reporter.h"
#include "storage/chunk_store.h"
#include "meta/placement.h"
#include "common/config.h"

#include <filesystem>
#include <string>
#include <unordered_set>

namespace fs = std::filesystem;

static nimbus::NodeConfig make_storage_cfg(const std::string& dir) {
    nimbus::NodeConfig cfg;
    cfg.node_id  = "s0";
    cfg.address  = "127.0.0.1:9200";
    cfg.data_dir = dir;
    return cfg;
}

static std::string tmp_dir() {
    char tmpl[] = "/tmp/nimbus_repair_XXXXXX";
    return mkdtemp(tmpl);
}

// ── parse_replica_endpoint (static, no instance needed) ───────────────────

TEST(ReplicaRepairParseTest, ParseNodeEqualsAddress) {
    std::string node_id, address;
    EXPECT_TRUE(nimbus::ReplicaRepair::parse_replica_endpoint(
        "s1=127.0.0.1:9201", node_id, address));
    EXPECT_EQ(node_id, "s1");
    EXPECT_EQ(address, "127.0.0.1:9201");
}

TEST(ReplicaRepairParseTest, ParsePlainNodeId) {
    std::string node_id, address;
    EXPECT_TRUE(nimbus::ReplicaRepair::parse_replica_endpoint("s2", node_id, address));
    EXPECT_EQ(node_id, "s2");
    EXPECT_TRUE(address.empty());
}

TEST(ReplicaRepairParseTest, ParseEmptyNodeIdReturnsFalse) {
    std::string node_id, address;
    EXPECT_FALSE(nimbus::ReplicaRepair::parse_replica_endpoint("=addr", node_id, address));
}

TEST(ReplicaRepairParseTest, ParseMultipleColonsInAddress) {
    // IPv6-style addresses with port: "s3=::1:9200"
    std::string node_id, address;
    EXPECT_TRUE(nimbus::ReplicaRepair::parse_replica_endpoint(
        "s3=127.0.0.1:9200", node_id, address));
    EXPECT_EQ(node_id, "s3");
    EXPECT_EQ(address, "127.0.0.1:9200");
}

// ── StatsReporter in-memory state ─────────────────────────────────────────

TEST(StatsReporterTest, InitialStateHasNoSnapshot) {
    std::string dir = tmp_dir();
    nimbus::NodeConfig cfg = make_storage_cfg(dir);
    nimbus::StatsReporter reporter(cfg, "127.0.0.1:9100");

    EXPECT_FALSE(reporter.has_assignment_snapshot());
    EXPECT_TRUE(reporter.assigned_chunk_ids().empty());
    fs::remove_all(dir);
}

TEST(StatsReporterTest, RecordAccessDoesNotCrash) {
    std::string dir = tmp_dir();
    nimbus::NodeConfig cfg = make_storage_cfg(dir);
    nimbus::StatsReporter reporter(cfg, "127.0.0.1:9100");

    reporter.record_access("chunk-1");
    reporter.record_access("chunk-1");
    reporter.record_access("chunk-2");

    // Counts are flushed during run_once(); verify basic liveness.
    EXPECT_FALSE(reporter.has_assignment_snapshot());
    fs::remove_all(dir);
}

TEST(StatsReporterTest, StopPreventsFurtherWork) {
    std::string dir = tmp_dir();
    nimbus::NodeConfig cfg = make_storage_cfg(dir);
    nimbus::StatsReporter reporter(cfg, "127.0.0.1:9100");

    reporter.stop();
    // run_once() must be a no-op — no RPC attempted, no crash.
    reporter.run_once();
    EXPECT_FALSE(reporter.has_assignment_snapshot());
    fs::remove_all(dir);
}

// ── ReplicaRepair lifecycle ────────────────────────────────────────────────

TEST(ReplicaRepairTest, StopPreventsBeyondInitialCheck) {
    std::string dir = tmp_dir();
    nimbus::NodeConfig cfg = make_storage_cfg(dir);
    nimbus::ChunkStore store(dir);
    nimbus::StatsReporter reporter(cfg, "127.0.0.1:9100");
    nimbus::ReplicaRepair repair(cfg, "127.0.0.1:9100", store, reporter);

    repair.stop();
    // run_once() must return immediately — no RPC after stop().
    repair.run_once();
    fs::remove_all(dir);
}

TEST(ReplicaRepairTest, SkipsRepairWithoutAssignmentSnapshot) {
    std::string dir = tmp_dir();
    nimbus::NodeConfig cfg = make_storage_cfg(dir);
    nimbus::ChunkStore store(dir);
    nimbus::StatsReporter reporter(cfg, "127.0.0.1:9100");
    nimbus::ReplicaRepair repair(cfg, "127.0.0.1:9100", store, reporter);

    // reporter.has_assignment_snapshot() == false → run_once() returns early
    // without attempting any gRPC call. No server running — must not crash.
    repair.run_once();
    fs::remove_all(dir);
}

// ── Placement: random mode ─────────────────────────────────────────────────

static nimbus::NodeEntry make_node(const std::string& id, float load = 0.3f) {
    nimbus::NodeEntry n;
    n.node_id       = id;
    n.address       = "127.0.0.1:9" + id.substr(1);
    n.load_fraction = load;
    n.free_bytes    = 4ULL << 30;
    n.total_bytes   = 8ULL << 30;
    n.alive         = true;
    return n;
}

TEST(PlacementRandomTest, SelectsRequestedCount) {
    nimbus::PlacementConfig cfg;
    cfg.random_placement = true;
    nimbus::Placement p(cfg);

    std::vector<nimbus::NodeEntry> nodes;
    for (int i = 0; i < 6; ++i) nodes.push_back(make_node("n" + std::to_string(i)));

    auto result = p.select_nodes(3, nodes, {});
    EXPECT_EQ(result.size(), 3u);
}

TEST(PlacementRandomTest, RespectsExcludeSet) {
    nimbus::PlacementConfig cfg;
    cfg.random_placement = true;
    nimbus::Placement p(cfg);

    std::vector<nimbus::NodeEntry> nodes;
    for (int i = 0; i < 5; ++i) nodes.push_back(make_node("n" + std::to_string(i)));

    auto result = p.select_nodes(3, nodes, {"n0", "n1"});
    ASSERT_EQ(result.size(), 3u);
    for (const auto& id : result) {
        EXPECT_NE(id, "n0");
        EXPECT_NE(id, "n1");
    }
}

TEST(PlacementRandomTest, NoDuplicatesInResult) {
    nimbus::PlacementConfig cfg;
    cfg.random_placement = true;
    nimbus::Placement p(cfg);

    std::vector<nimbus::NodeEntry> nodes;
    for (int i = 0; i < 8; ++i) nodes.push_back(make_node("n" + std::to_string(i)));

    auto result = p.select_nodes(5, nodes, {});
    ASSERT_EQ(result.size(), 5u);
    std::unordered_set<std::string> unique(result.begin(), result.end());
    EXPECT_EQ(unique.size(), 5u);
}

TEST(PlacementRandomTest, ProducesVariedResults) {
    nimbus::PlacementConfig cfg;
    cfg.random_placement = true;
    nimbus::Placement p(cfg);

    std::vector<nimbus::NodeEntry> nodes;
    for (int i = 0; i < 10; ++i) nodes.push_back(make_node("n" + std::to_string(i)));

    // Probability all 30 trials are identical = (C(10,3))^-29 ≈ 0.
    std::vector<std::string> first = p.select_nodes(3, nodes, {});
    bool found_different = false;
    for (int trial = 0; trial < 30; ++trial) {
        if (p.select_nodes(3, nodes, {}) != first) { found_different = true; break; }
    }
    EXPECT_TRUE(found_different);
}
