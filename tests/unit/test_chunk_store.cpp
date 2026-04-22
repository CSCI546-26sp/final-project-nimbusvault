#include <gtest/gtest.h>
#include "../../src/storage/chunk_store.h"
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

class ChunkStoreTest : public ::testing::Test {
protected:
    std::string dir_;

    void SetUp() override {
        dir_ = "/tmp/nimbus_chunk_store_test_" +
               std::to_string(reinterpret_cast<uintptr_t>(this));
        fs::create_directories(dir_);
    }

    void TearDown() override {
        fs::remove_all(dir_);
    }
};

TEST_F(ChunkStoreTest, WriteAndReadBack) {
    nimbus::ChunkStore store(dir_);
    ASSERT_TRUE(store.write("c1", 1, "hello"));

    std::string out;
    uint64_t ver = 0;
    ASSERT_TRUE(store.read("c1", 1, out, ver));
    EXPECT_EQ(out, "hello");
    EXPECT_EQ(ver, 1u);
}

TEST_F(ChunkStoreTest, MinVersionEnforced) {
    nimbus::ChunkStore store(dir_);
    ASSERT_TRUE(store.write("c1", 1, "v1"));

    std::string out;
    uint64_t ver = 0;
    EXPECT_FALSE(store.read("c1", 2, out, ver));
}

TEST_F(ChunkStoreTest, ReadReturnsLatestVersion) {
    nimbus::ChunkStore store(dir_);
    ASSERT_TRUE(store.write("c1", 1, "v1"));
    ASSERT_TRUE(store.write("c1", 2, "v2"));

    std::string out;
    uint64_t ver = 0;
    ASSERT_TRUE(store.read("c1", 1, out, ver));
    EXPECT_EQ(out, "v2");
    EXPECT_EQ(ver, 2u);
}

TEST_F(ChunkStoreTest, RemoveDeletesChunk) {
    nimbus::ChunkStore store(dir_);
    ASSERT_TRUE(store.write("c1", 1, "data"));
    ASSERT_TRUE(store.remove("c1"));

    std::string out;
    uint64_t ver = 0;
    EXPECT_FALSE(store.read("c1", 1, out, ver));
}

TEST_F(ChunkStoreTest, LatestVersionReturnsZeroIfMissing) {
    nimbus::ChunkStore store(dir_);
    EXPECT_EQ(store.latest_version("missing"), 0u);
}

TEST_F(ChunkStoreTest, MultipleChunksIndependent) {
    nimbus::ChunkStore store(dir_);
    ASSERT_TRUE(store.write("chunk_a", 1, "A"));
    ASSERT_TRUE(store.write("chunk_b", 1, "B"));

    std::string out_a;
    std::string out_b;
    uint64_t ver_a = 0;
    uint64_t ver_b = 0;

    ASSERT_TRUE(store.read("chunk_a", 1, out_a, ver_a));
    ASSERT_TRUE(store.read("chunk_b", 1, out_b, ver_b));

    EXPECT_EQ(out_a, "A");
    EXPECT_EQ(out_b, "B");
    EXPECT_EQ(ver_a, 1u);
    EXPECT_EQ(ver_b, 1u);
}
