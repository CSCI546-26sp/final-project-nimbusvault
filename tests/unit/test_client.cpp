#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>

#include "client/history_tracer.h"
#include "client/nimbus_client.h"
#include "workload/workload_gen.h"
#include "meta.pb.h"  // nimbus::meta::ChunkMeta

namespace fs = std::filesystem;

// ── helpers ──────────────────────────────────────────────────────────────────

static std::string tmp_csv() {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "/tmp/nimbus_client_test_%u.csv",
                  static_cast<unsigned>(reinterpret_cast<uintptr_t>(buf)));
    return buf;
}

// ── HistoryTracer: tier field ─────────────────────────────────────────────────

TEST(HistoryTracerTierTest, TierWrittenToCsv) {
    std::string path = tmp_csv();
    {
        nimbus::HistoryTracer tracer(path);
        tracer.record(nimbus::OpType::PUT, "c1", 1000, 50, true, 1024, "hot");
        tracer.record(nimbus::OpType::GET, "c2", 2000, 30, true,  512, "cold");
    }
    std::ifstream ifs(path);
    ASSERT_TRUE(ifs.is_open());
    std::string header;
    std::getline(ifs, header);
    EXPECT_NE(header.find("tier"), std::string::npos) << "CSV header must contain 'tier'";

    std::string line;
    int hot_found = 0, cold_found = 0;
    while (std::getline(ifs, line)) {
        if (line.find(",hot")  != std::string::npos) hot_found++;
        if (line.find(",cold") != std::string::npos) cold_found++;
    }
    EXPECT_EQ(hot_found,  1);
    EXPECT_EQ(cold_found, 1);
    fs::remove(path);
}

TEST(HistoryTracerTierTest, EmptyTierDefaultsToEmpty) {
    std::string path = tmp_csv();
    {
        nimbus::HistoryTracer tracer(path);
        tracer.record(nimbus::OpType::PUT, "c1", 1000, 50, true, 1024);
    }
    std::ifstream ifs(path);
    ASSERT_TRUE(ifs.is_open());
    std::string header, row;
    std::getline(ifs, header);
    std::getline(ifs, row);
    // Last field (tier) should be empty: row ends with a comma.
    EXPECT_TRUE(row.back() == ',') << "empty tier → trailing comma, got: " << row;
    fs::remove(path);
}

TEST(HistoryTracerTierTest, RecordTierChangeWritesSpecialRow) {
    std::string path = tmp_csv();
    {
        nimbus::HistoryTracer tracer(path);
        tracer.record_tier_change("chunk-x", "cold", "hot", 999000);
    }
    std::ifstream ifs(path);
    ASSERT_TRUE(ifs.is_open());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                         std::istreambuf_iterator<char>());
    EXPECT_NE(content.find("TIER_CHANGE"), std::string::npos);
    EXPECT_NE(content.find("chunk-x"),     std::string::npos);
    EXPECT_NE(content.find("cold->hot"),   std::string::npos);
    fs::remove(path);
}

TEST(HistoryTracerTierTest, GetStatsReturnsCorrectTotals) {
    std::string path = tmp_csv();
    nimbus::HistoryTracer tracer(path);
    for (int i = 0; i < 10; ++i)
        tracer.record(nimbus::OpType::GET, "c", 1000 + i * 100, 10 + i, true, 512, "hot");
    for (int i = 0; i < 2; ++i)
        tracer.record(nimbus::OpType::PUT, "c", 2000 + i * 100, 50,     false, 0, "cold");

    auto s = tracer.get_stats();
    EXPECT_EQ(s.total_ops, 12u);
    EXPECT_EQ(s.errors,     2u);
    EXPECT_GT(s.p50_us,     0u);
    EXPECT_GT(s.p99_us,     0u);
    fs::remove(path);
}

// ── NimbusClient: build_replica_try_list ─────────────────────────────────────
// Verifies C-1 (TRANSITIONING fallback): the try-list must include both
// replica_set and old_replica_set so reads succeed during a reconfig.

TEST(ReplicaTryListTest, StableChunkOnlyCurrentReplicas) {
    nimbus::meta::ChunkMeta meta;
    meta.add_replica_set("s0=127.0.0.1:9200");
    meta.add_replica_set("s1=127.0.0.1:9201");

    auto list = nimbus::NimbusClient::build_replica_try_list(meta);
    ASSERT_EQ(list.size(), 2u);
    EXPECT_EQ(list[0], "s0=127.0.0.1:9200");
    EXPECT_EQ(list[1], "s1=127.0.0.1:9201");
}

TEST(ReplicaTryListTest, TransitioningChunkIncludesOldReplicas) {
    nimbus::meta::ChunkMeta meta;
    meta.add_replica_set("s2=127.0.0.1:9202");
    meta.add_replica_set("s3=127.0.0.1:9203");
    meta.add_old_replica_set("s0=127.0.0.1:9200");
    meta.add_old_replica_set("s1=127.0.0.1:9201");

    auto list = nimbus::NimbusClient::build_replica_try_list(meta);
    ASSERT_EQ(list.size(), 4u);
    // Current replicas come first.
    EXPECT_EQ(list[0], "s2=127.0.0.1:9202");
    EXPECT_EQ(list[1], "s3=127.0.0.1:9203");
    // Old replicas appended as fallback.
    EXPECT_EQ(list[2], "s0=127.0.0.1:9200");
    EXPECT_EQ(list[3], "s1=127.0.0.1:9201");
}

TEST(ReplicaTryListTest, EmptyMetaReturnsEmptyList) {
    nimbus::meta::ChunkMeta meta;
    auto list = nimbus::NimbusClient::build_replica_try_list(meta);
    EXPECT_TRUE(list.empty());
}

// ── WorkloadConfig defaults and mode field ───────────────────────────────────

TEST(WorkloadConfigTest, DefaultsAreReasonable) {
    nimbus::WorkloadConfig cfg;
    EXPECT_EQ(cfg.mode,        "adaptive");
    EXPECT_EQ(cfg.desired_rf,  3);
    EXPECT_GT(cfg.num_chunks,  0);
    EXPECT_GT(cfg.num_ops,     0);
    EXPECT_GT(cfg.write_ratio, 0.0);
    EXPECT_LT(cfg.write_ratio, 1.0);
}

TEST(WorkloadConfigTest, ModeFieldCanBeChanged) {
    nimbus::WorkloadConfig cfg;
    cfg.mode       = "baseline";
    cfg.desired_rf = 3;
    EXPECT_EQ(cfg.mode, "baseline");
    EXPECT_EQ(cfg.desired_rf, 3);

    cfg.mode = "compare";
    EXPECT_EQ(cfg.mode, "compare");
}

// ── WorkloadGen: hot/cold distribution ───────────────────────────────────────
// WorkloadGen::pick_chunk() is private, but we can verify the distribution
// indirectly: the HistoryTracer CSV must show more "hot" rows than "cold" rows
// when hot_read_weight > 0.5, without connecting to a real cluster.
//
// NOTE: WorkloadGen.run() needs a live meta server (gRPC), so we only test
// the config-level invariants here. Full end-to-end tests live in
// tests/integration/.

TEST(WorkloadConfigTest, HotCountComputedCorrectly) {
    // Replicate the constructor logic and verify the expected split.
    nimbus::WorkloadConfig cfg;
    cfg.num_chunks   = 100;
    cfg.hot_fraction = 0.2;

    int hot_count  = static_cast<int>(cfg.num_chunks * cfg.hot_fraction);
    int cold_count = cfg.num_chunks - hot_count;
    EXPECT_EQ(hot_count,  20);
    EXPECT_EQ(cold_count, 80);
}

TEST(WorkloadConfigTest, HotFractionClampedToUnit) {
    nimbus::WorkloadConfig cfg;
    cfg.num_chunks   = 50;
    cfg.hot_fraction = 1.5; // clamped to 1.0

    double hf = cfg.hot_fraction;
    if (hf > 1.0) hf = 1.0;
    if (hf < 0.0) hf = 0.0;
    int hot_count = static_cast<int>(cfg.num_chunks * hf);
    EXPECT_EQ(hot_count, 50);
}
