#include <gtest/gtest.h>
#include "../../src/meta/placement.h"
#include "../../src/common/config.h"
#include <unordered_set>

static nimbus::PlacementConfig default_cfg() {
    return {0.3, 0.3, 0.2, 0.2};
}

static nimbus::NodeEntry make_node(const std::string& id,
                                    float load, uint64_t free_gb,
                                    float fail_rate = 0.0f) {
    nimbus::NodeEntry n;
    n.node_id         = id;
    n.address         = "127.0.0.1:9200";
    n.load_fraction   = load;
    n.free_bytes      = free_gb * (1ULL << 30);
    n.total_bytes     = 10ULL << 30;
    n.failure_rate_7d = fail_rate;
    n.last_seen_ms    = 1000;
    n.alive           = true;
    return n;
}

TEST(PlacementTest, SelectsRequestedCount) {
    nimbus::Placement p(default_cfg());
    std::vector<nimbus::NodeEntry> nodes = {
        make_node("n1", 0.1f, 8),
        make_node("n2", 0.2f, 7),
        make_node("n3", 0.3f, 6),
        make_node("n4", 0.4f, 5),
    };
    auto result = p.select_nodes(3, nodes, {});
    EXPECT_EQ(result.size(), 3u);
}

TEST(PlacementTest, RespectsExcludeSet) {
    nimbus::Placement p(default_cfg());
    std::vector<nimbus::NodeEntry> nodes = {
        make_node("n1", 0.1f, 8),
        make_node("n2", 0.2f, 7),
        make_node("n3", 0.3f, 6),
    };
    auto result = p.select_nodes(2, nodes, {"n1"});
    ASSERT_EQ(result.size(), 2u);
    for (auto& id : result)
        EXPECT_NE(id, "n1");
}

TEST(PlacementTest, PrefersLowLoadNode) {
    nimbus::Placement p(default_cfg());
    std::vector<nimbus::NodeEntry> nodes = {
        make_node("n1", 0.05f, 5),
        make_node("n2", 0.80f, 5),
        make_node("n3", 0.90f, 5),
    };
    auto result = p.select_nodes(1, nodes, {});
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], "n1");
}

TEST(PlacementTest, PrefersHighCapacityNode) {
    nimbus::Placement p(default_cfg());
    // Equal load — n3 has most free space.
    std::vector<nimbus::NodeEntry> nodes = {
        make_node("n1", 0.5f, 2),
        make_node("n2", 0.5f, 4),
        make_node("n3", 0.5f, 9),
    };
    auto result = p.select_nodes(1, nodes, {});
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], "n3");
}

TEST(PlacementTest, AvoidsHighFailureRateNode) {
    nimbus::Placement p(default_cfg());
    std::vector<nimbus::NodeEntry> nodes = {
        make_node("n1", 0.3f, 5, 0.0f),
        make_node("n2", 0.3f, 5, 0.9f),  
    };
    auto result = p.select_nodes(1, nodes, {});
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], "n1");
}

TEST(PlacementTest, ReturnsFewerNodesWhenNotEnoughAvailable) {
    nimbus::Placement p(default_cfg());
    std::vector<nimbus::NodeEntry> nodes = {make_node("n1", 0.1f, 5)};
    auto result = p.select_nodes(3, nodes, {});
    EXPECT_EQ(result.size(), 1u);  
}

TEST(PlacementTest, DiversityPreventsRepeatSelection) {
    nimbus::Placement p(default_cfg());
    std::vector<nimbus::NodeEntry> nodes = {
        make_node("n1", 0.1f, 8),
        make_node("n2", 0.1f, 8),
        make_node("n3", 0.1f, 8),
    };
    auto result = p.select_nodes(3, nodes, {});
    std::unordered_set<std::string> unique(result.begin(), result.end());
    EXPECT_EQ(unique.size(), 3u);
}
