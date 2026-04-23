#include <gtest/gtest.h>
#include "meta/meta_coordinator.h"
#include "meta/meta_wal.h"
#include "meta/meta_snapshot.h"
#include "meta/policy.h"
#include "common/config.h"

#include <filesystem>
#include <cstdio>

namespace fs = std::filesystem;

static nimbus::NodeConfig make_node_cfg(const std::string& data_dir) {
    nimbus::NodeConfig cfg;
    cfg.node_id = "m0";
    cfg.address = "127.0.0.1:9100";
    cfg.data_dir = data_dir;
    return cfg;
}

static std::string tmp_dir() {
    char tmpl[] = "/tmp/nimbus_integ_XXXXXX";
    return mkdtemp(tmpl);
}

// ── Snapshot round-trip ────────────────────────────────────────────────────

TEST(MetaIntegration, SnapshotRoundTrip) {
    std::string dir = tmp_dir();

    nimbus::NodeConfig cfg = make_node_cfg(dir);
    nimbus::MetaWal wal(dir);
    nimbus::MetaCoordinator coord(cfg);
    nimbus::MetaSnapshot snap(coord, wal, dir);

    // Populate coordinator with a node and a chunk.
    coord.apply_heartbeat("s0", "127.0.0.1:9200", 0.1f, 1000000, 2000000, 0.0f);
    coord.apply_put_chunk("chunk-1", 1024, 3, {"s0=127.0.0.1:9200"}, 1);

    // Take snapshot.
    auto [data, lsn] = snap.take_snapshot();
    EXPECT_FALSE(data.empty());

    // Create a fresh coordinator and install the snapshot.
    nimbus::MetaCoordinator coord2(cfg);
    nimbus::MetaSnapshot snap2(coord2, wal, dir);
    uint64_t restored_lsn = snap2.install_snapshot(data);
    EXPECT_EQ(restored_lsn, lsn);

    // Verify chunk was restored.
    const auto* entry = coord2.get_chunk("chunk-1");
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->chunk_id, "chunk-1");
    EXPECT_EQ(entry->version, 1u);
    EXPECT_EQ(entry->replication_factor, 3);
    EXPECT_EQ(entry->replica_set.size(), 1u);

    // Verify node was restored.
    auto nodes = coord2.get_nodes();
    ASSERT_EQ(nodes.size(), 1u);
    EXPECT_EQ(nodes[0].node_id, "s0");
    EXPECT_EQ(nodes[0].address, "127.0.0.1:9200");

    fs::remove_all(dir);
}

TEST(MetaIntegration, SnapshotWithChunkState) {
    std::string dir = tmp_dir();
    nimbus::NodeConfig cfg = make_node_cfg(dir);
    nimbus::MetaWal wal(dir);
    nimbus::MetaCoordinator coord(cfg);
    nimbus::MetaSnapshot snap(coord, wal, dir);

    coord.apply_put_chunk("c1", 512, 2, {"n0=addr0", "n1=addr1"}, 5);
    coord.apply_put_chunk("c2", 256, 3, {"n0=addr0"}, 2);
    coord.apply_reconfig_start("c1", {"n0=addr0", "n1=addr1"}, {"n2=addr2", "n3=addr3"});

    auto [data, lsn] = snap.take_snapshot();

    nimbus::MetaCoordinator coord2(cfg);
    nimbus::MetaSnapshot snap2(coord2, wal, dir);
    snap2.install_snapshot(data);

    const auto* c1 = coord2.get_chunk("c1");
    ASSERT_NE(c1, nullptr);
    EXPECT_EQ(c1->config_state, nimbus::ChunkConfigState::TRANSITIONING);
    EXPECT_EQ(c1->old_replica_set.size(), 2u);

    const auto* c2 = coord2.get_chunk("c2");
    ASSERT_NE(c2, nullptr);
    EXPECT_EQ(c2->config_state, nimbus::ChunkConfigState::STABLE);

    fs::remove_all(dir);
}

// ── Policy tier change ─────────────────────────────────────────────────────

static nimbus::PolicyConfig fast_policy_cfg() {
    nimbus::PolicyConfig cfg;
    cfg.cold_threshold   = 0.1;
    cfg.hot_threshold    = 1.0;
    cfg.cold_rf          = 2;
    cfg.warm_rf          = 3;
    cfg.hot_rf           = 5;
    cfg.ewma_half_life_s = 1.0;
    cfg.promote_windows  = 2;
    cfg.demote_windows   = 3;
    return cfg;
}

TEST(MetaIntegration, PolicyColdChunkNoChange) {
    nimbus::AdaptivePolicy policy(fast_policy_cfg());
    policy.record_accesses("c1", 0, 1000);
    auto dec = policy.evaluate("c1");
    EXPECT_EQ(dec.new_rf, 2);
    EXPECT_EQ(dec.new_tier, nimbus::ChunkTier::COLD);
}

TEST(MetaIntegration, PolicyHotPromoteAndDemote) {
    nimbus::AdaptivePolicy policy(fast_policy_cfg());

    // Promote to HOT after 2 consecutive hot windows.
    policy.record_accesses("c1", 100, 1000);
    auto d1 = policy.evaluate("c1");
    EXPECT_FALSE(d1.changed);

    policy.record_accesses("c1", 100, 1000);
    auto d2 = policy.evaluate("c1");
    EXPECT_TRUE(d2.changed);
    EXPECT_EQ(d2.new_rf, 5);
    EXPECT_EQ(d2.new_tier, nimbus::ChunkTier::HOT);

    // Should not demote under hysteresis (needs demote_windows=3 cold windows).
    policy.record_accesses("c1", 0, 1000);
    EXPECT_FALSE(policy.evaluate("c1").changed);

    policy.record_accesses("c1", 0, 1000);
    EXPECT_FALSE(policy.evaluate("c1").changed);

    // Third cold window triggers demotion.
    policy.record_accesses("c1", 0, 1000);
    auto d3 = policy.evaluate("c1");
    EXPECT_TRUE(d3.changed);
    EXPECT_EQ(d3.new_tier, nimbus::ChunkTier::COLD);
}

// ── WAL read_entry round-trip (prerequisite for FetchLogEntries logic) ─────

TEST(MetaIntegration, WalReadEntryRoundTrip) {
    std::string dir = tmp_dir();
    {
        nimbus::MetaWal wal(dir);
        uint64_t idx = wal.append(nimbus::WalEntryType::PUT_CHUNK, 1, "chunk-42|1024|3|1|n0,");
        wal.commit(idx);

        nimbus::WalEntry e;
        bool ok = wal.read_entry(idx, e);
        ASSERT_TRUE(ok);
        EXPECT_EQ(e.log_index, idx);
        EXPECT_EQ(e.type, nimbus::WalEntryType::PUT_CHUNK);
        EXPECT_NE(e.payload.find("chunk-42"), std::string::npos);
        EXPECT_EQ(wal.last_log_index(), idx);
    }
    fs::remove_all(dir);
}

TEST(MetaIntegration, WalFetchRangeReadable) {
    std::string dir = tmp_dir();
    {
        nimbus::MetaWal wal(dir);
        uint64_t first = 0, last = 0;
        for (int i = 0; i < 5; ++i) {
            uint64_t idx = wal.append(nimbus::WalEntryType::PUT_CHUNK, 1,
                                      "c" + std::to_string(i) + "|0|2|" + std::to_string(i) + "|");
            wal.commit(idx);
            if (i == 0) first = idx;
            last = idx;
        }

        // Simulate FetchLogEntries: read every entry from first to last.
        for (uint64_t idx = first; idx <= last; ++idx) {
            nimbus::WalEntry e;
            ASSERT_TRUE(wal.read_entry(idx, e)) << "missing entry at idx=" << idx;
            EXPECT_EQ(e.log_index, idx);
        }
    }
    fs::remove_all(dir);
}
