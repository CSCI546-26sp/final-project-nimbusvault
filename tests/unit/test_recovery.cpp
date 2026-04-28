#include <gtest/gtest.h>

#include "meta/recovery.h"
#include "meta/meta_coordinator.h"
#include "meta/meta_wal.h"
#include "common/config.h"

#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;

static std::string tmp_dir() {
    char tmpl[] = "/tmp/nimbus_recovery_XXXXXX";
    return mkdtemp(tmpl);
}

static nimbus::NodeConfig make_cfg(const std::string& dir) {
    nimbus::NodeConfig cfg;
    cfg.node_id = "meta0";
    cfg.address = "127.0.0.1:9100";
    cfg.data_dir = dir;
    return cfg;
}

TEST(RecoveryTest, CleanupUncommittedPutsDeletesReplicaBytes) {
    std::string dir = tmp_dir();
    nimbus::MetaWal wal(dir);
    nimbus::MetaCoordinator coord(make_cfg(dir));

    coord.apply_heartbeat("n1", "127.0.0.1:9201", 0.1f, 1 << 30, 2 << 30, 0.0f);
    coord.apply_heartbeat("n2", "127.0.0.1:9202", 0.2f, 1 << 30, 2 << 30, 0.0f);

    uint64_t committed_idx = wal.append(nimbus::WalEntryType::PUT_CHUNK, 1,
                                        "committed|4|2|1|n1=127.0.0.1:9201,n2=127.0.0.1:9202,");
    wal.commit(committed_idx);

    uint64_t uncommitted_idx = wal.append(nimbus::WalEntryType::PUT_CHUNK, 1,
                                          "dangling|4|2|2|n1=127.0.0.1:9201,n2=127.0.0.1:9202,");
    (void)uncommitted_idx;

    std::vector<std::pair<std::string, std::string>> calls;
    nimbus::cleanup_uncommitted_puts(wal, coord, [&](const std::string& addr,
                                                    const std::string& chunk_id) {
        calls.emplace_back(addr, chunk_id);
    });

    ASSERT_EQ(calls.size(), 2u);
    EXPECT_EQ(calls[0].second, "dangling");
    EXPECT_EQ(calls[1].second, "dangling");
    EXPECT_TRUE((calls[0].first == "127.0.0.1:9201" && calls[1].first == "127.0.0.1:9202") ||
                (calls[0].first == "127.0.0.1:9202" && calls[1].first == "127.0.0.1:9201"));

    fs::remove_all(dir);
}
