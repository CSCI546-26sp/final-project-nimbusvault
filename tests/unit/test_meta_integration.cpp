#include <gtest/gtest.h>

#include "../../src/common/config.h"
#include "../../src/meta/meta_coordinator.h"
#include "../../src/meta/meta_follower.h"
#include "../../src/meta/meta_rpc.h"
#include "../../src/meta/meta_snapshot.h"
#include "../../src/meta/meta_wal.h"
#include "../../src/meta/policy.h"

#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <memory>

namespace fs = std::filesystem;

static nimbus::NodeConfig make_cfg(const std::string& dir) {
    nimbus::NodeConfig cfg;
    cfg.node_id = "meta0";
    cfg.role = "leader";
    cfg.address = "127.0.0.1:0";
    cfg.data_dir = dir;
    cfg.mode = "adaptive";
    return cfg;
}

class MetaIntegrationTest : public ::testing::Test {
protected:
    std::string dir_;

    void SetUp() override {
        dir_ = "/tmp/nimbus_meta_integration_" +
               std::to_string(reinterpret_cast<uintptr_t>(this));
        fs::create_directories(dir_);
    }

    void TearDown() override {
        fs::remove_all(dir_);
    }
};

TEST_F(MetaIntegrationTest, SnapshotRoundTripRestoresCoordinatorState) {
    nimbus::NodeConfig cfg = make_cfg(dir_ + "/src");
    fs::create_directories(cfg.data_dir);

    nimbus::MetaCoordinator src_coord(cfg);
    nimbus::MetaWal src_wal(cfg.data_dir);
    nimbus::MetaSnapshot src_snapshot(src_coord, src_wal, cfg.data_dir);

    src_coord.apply_heartbeat("n1", "127.0.0.1:9200", 0.2f, 1000, 4000, 0.01f);
    src_coord.apply_heartbeat("n2", "127.0.0.1:9201", 0.3f, 1100, 4000, 0.02f);
    src_coord.apply_put_chunk("chunkA", 4096, 3, {"n1", "n2", "n3"}, 7);

    uint64_t idx = src_wal.append(nimbus::WalEntryType::PUT_CHUNK, 1, "seed");
    src_wal.commit(idx);

    auto snap = src_snapshot.take_snapshot();

    nimbus::NodeConfig dst_cfg = make_cfg(dir_ + "/dst");
    fs::create_directories(dst_cfg.data_dir);
    nimbus::MetaCoordinator dst_coord(dst_cfg);
    nimbus::MetaWal dst_wal(dst_cfg.data_dir);
    nimbus::MetaSnapshot dst_snapshot(dst_coord, dst_wal, dst_cfg.data_dir);

    uint64_t installed_lsn = dst_snapshot.install_snapshot(snap.first);
    EXPECT_EQ(installed_lsn, snap.second);

    auto nodes = dst_coord.get_nodes();
    EXPECT_EQ(nodes.size(), 2u);

    const nimbus::ChunkEntry* chunk = dst_coord.get_chunk("chunkA");
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->version, 7u);
    EXPECT_EQ(chunk->replication_factor, 3);
    EXPECT_EQ(chunk->replica_set.size(), 3u);
}

TEST_F(MetaIntegrationTest, PolicyEvaluateSignalsReconfigTrigger) {
    nimbus::PolicyConfig pcfg;
    pcfg.cold_threshold = 0.1;
    pcfg.hot_threshold = 1.0;
    pcfg.cold_rf = 2;
    pcfg.warm_rf = 3;
    pcfg.hot_rf = 5;
    pcfg.promote_windows = 2;
    pcfg.demote_windows = 3;
    pcfg.ewma_half_life_s = 1.0;

    nimbus::AdaptivePolicy policy(pcfg);
    policy.record_accesses("chunk-hot", 100, 1000);
    auto d1 = policy.evaluate("chunk-hot");
    EXPECT_FALSE(d1.changed);

    policy.record_accesses("chunk-hot", 100, 1000);
    auto d2 = policy.evaluate("chunk-hot");
    EXPECT_TRUE(d2.changed);
    EXPECT_EQ(d2.new_tier, nimbus::ChunkTier::HOT);
    EXPECT_EQ(d2.new_rf, 5);
}

TEST_F(MetaIntegrationTest, FetchLogEntriesStreamsFromRequestedIndex) {
    nimbus::NodeConfig cfg = make_cfg(dir_ + "/repl");
    fs::create_directories(cfg.data_dir);

    nimbus::MetaCoordinator coord(cfg);
    nimbus::MetaWal wal(cfg.data_dir);
    nimbus::MetaFollower follower(wal, cfg, nullptr);
    nimbus::MetaSnapshot snapshot(coord, wal, cfg.data_dir);
    nimbus::MetaReplService service(follower, snapshot, nullptr);

    uint64_t i1 = wal.append(nimbus::WalEntryType::PUT_CHUNK, 1, "p1");
    uint64_t i2 = wal.append(nimbus::WalEntryType::DELETE_CHUNK, 1, "p2");
    uint64_t i3 = wal.append(nimbus::WalEntryType::RECONFIGURE_START, 1, "p3");
    wal.commit(i1);
    wal.commit(i2);
    wal.commit(i3);

    int selected_port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &selected_port);
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);
    ASSERT_GT(selected_port, 0);

    auto channel = grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(selected_port),
        grpc::InsecureChannelCredentials());
    auto stub = nimbus::repl::MetaReplication::NewStub(channel);

    nimbus::repl::FetchLogReq req;
    req.set_from_index(2);

    grpc::ClientContext ctx;
    auto reader = stub->FetchLogEntries(&ctx, req);
    std::vector<nimbus::repl::LogEntry> out;
    nimbus::repl::LogEntry entry;
    while (reader->Read(&entry)) {
        out.push_back(entry);
    }
    grpc::Status status = reader->Finish();
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(out.size(), 2u);
    ASSERT_GE(out.size(), 2u);
    EXPECT_EQ(out[0].log_index(), 2u);
    EXPECT_EQ(out[1].log_index(), 3u);
    EXPECT_EQ(out[0].payload(), "p2");
    EXPECT_EQ(out[1].payload(), "p3");

    server->Shutdown();
}
