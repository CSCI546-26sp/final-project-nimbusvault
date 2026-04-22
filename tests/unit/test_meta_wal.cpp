#include <gtest/gtest.h>
#include "../../src/meta/meta_wal.h"
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

class WalTest : public ::testing::Test {
protected:
    std::string dir_;
    void SetUp() override {
        dir_ = "/tmp/nimbus_wal_test_" +
               std::to_string(reinterpret_cast<uintptr_t>(this));
        fs::create_directories(dir_ + "/wal");
    }
    void TearDown() override { fs::remove_all(dir_); }
};

TEST_F(WalTest, AppendIncreasesLastIndex) {
    nimbus::MetaWal wal(dir_);
    EXPECT_EQ(wal.last_log_index(), 0u);
    uint64_t idx = wal.append(nimbus::WalEntryType::PUT_CHUNK, 1, "payload1");
    EXPECT_EQ(idx, 1u);
    EXPECT_EQ(wal.last_log_index(), 1u);
}

TEST_F(WalTest, CommitAdvancesCommittedIndex) {
    nimbus::MetaWal wal(dir_);
    wal.append(nimbus::WalEntryType::PUT_CHUNK, 1, "p1");
    EXPECT_EQ(wal.committed_index(), 0u);
    wal.commit(1);
    EXPECT_EQ(wal.committed_index(), 1u);
}

TEST_F(WalTest, UncommittedEntryNotReplayed) {
    nimbus::MetaWal wal(dir_);
    wal.append(nimbus::WalEntryType::PUT_CHUNK, 1, "uncommitted");
    int replayed = 0;
    wal.replay([&](const nimbus::WalEntry&) { replayed++; });
    EXPECT_EQ(replayed, 0);
}

TEST_F(WalTest, ReplayRestoresCommittedEntries) {
    {
        nimbus::MetaWal wal(dir_);
        for (int i = 0; i < 3; ++i) {
            uint64_t idx = wal.append(nimbus::WalEntryType::PUT_CHUNK, 1,
                                       "entry" + std::to_string(i));
            wal.commit(idx);
        }
        wal.append(nimbus::WalEntryType::DELETE_CHUNK, 1, "uncommitted");
    }
    nimbus::MetaWal wal2(dir_);
    std::vector<nimbus::WalEntry> replayed;
    wal2.replay([&](const nimbus::WalEntry& e) { replayed.push_back(e); });
    ASSERT_EQ(replayed.size(), 3u);
    EXPECT_EQ(replayed[0].payload, "entry0");
    EXPECT_EQ(replayed[2].payload, "entry2");
}

TEST_F(WalTest, EntryOrderingPreserved) {
    nimbus::MetaWal wal(dir_);
    for (int i = 1; i <= 5; ++i) {
        uint64_t idx = wal.append(nimbus::WalEntryType::PUT_CHUNK, 1,
                                   std::to_string(i));
        wal.commit(idx);
    }
    std::vector<uint64_t> indices;
    wal.replay([&](const nimbus::WalEntry& e) { indices.push_back(e.log_index); });
    ASSERT_EQ(indices.size(), 5u);
    for (size_t i = 1; i < indices.size(); ++i)
        EXPECT_GT(indices[i], indices[i - 1]);
}

TEST_F(WalTest, MultipleEntryTypesRoundtrip) {
    nimbus::MetaWal wal(dir_);
    uint64_t i1 = wal.append(nimbus::WalEntryType::PUT_CHUNK,       1, "put");
    uint64_t i2 = wal.append(nimbus::WalEntryType::DELETE_CHUNK,    1, "del");
    uint64_t i3 = wal.append(nimbus::WalEntryType::RECONFIGURE_START, 1, "rs");
    wal.commit(i1); wal.commit(i2); wal.commit(i3);

    std::vector<nimbus::WalEntry> out;
    wal.replay([&](const nimbus::WalEntry& e) { out.push_back(e); });
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0].type, nimbus::WalEntryType::PUT_CHUNK);
    EXPECT_EQ(out[1].type, nimbus::WalEntryType::DELETE_CHUNK);
    EXPECT_EQ(out[2].type, nimbus::WalEntryType::RECONFIGURE_START);
}
