#include <gtest/gtest.h>
#include "../../src/meta/policy.h"
#include "../../src/common/config.h"

static nimbus::PolicyConfig make_cfg() {
    nimbus::PolicyConfig cfg;
    cfg.cold_threshold  = 0.1;
    cfg.hot_threshold   = 1.0;
    cfg.cold_rf         = 2;
    cfg.warm_rf         = 3;
    cfg.hot_rf          = 5;
    cfg.ewma_half_life_s = 60.0;
    cfg.promote_windows  = 2;
    cfg.demote_windows   = 3;
    return cfg;
}

TEST(PolicyTest, ColdChunkGetsColdRf) {
    nimbus::AdaptivePolicy policy(make_cfg());
    // 0 accesses in 1000ms window → rate = 0 → COLD
    policy.record_accesses("c1", 0, 1000);
    policy.record_accesses("c1", 0, 1000);
    auto dec = policy.evaluate("c1");
    EXPECT_EQ(dec.new_rf, 2);
    EXPECT_EQ(dec.new_tier, nimbus::ChunkTier::COLD);
}

TEST(PolicyTest, HotChunkPromotesAfterConsecutiveWindows) {
    nimbus::AdaptivePolicy policy(make_cfg());
    policy.record_accesses("c1", 100, 1000);
    auto d1 = policy.evaluate("c1");  
    EXPECT_FALSE(d1.changed);

    policy.record_accesses("c1", 100, 1000);
    auto d2 = policy.evaluate("c1");  
    EXPECT_TRUE(d2.changed);
    EXPECT_EQ(d2.new_rf, 5);
    EXPECT_EQ(d2.new_tier, nimbus::ChunkTier::HOT);
}

TEST(PolicyTest, NoDemotionUnderHysteresis) {
    nimbus::AdaptivePolicy policy(make_cfg());
    policy.record_accesses("c1", 100, 1000);
    policy.evaluate("c1");
    policy.record_accesses("c1", 100, 1000);
    policy.evaluate("c1");

    policy.record_accesses("c1", 0, 1000);
    auto d1 = policy.evaluate("c1");
    EXPECT_FALSE(d1.changed);

    policy.record_accesses("c1", 0, 1000);
    auto d2 = policy.evaluate("c1");
    EXPECT_FALSE(d2.changed);

    policy.record_accesses("c1", 0, 1000);
    auto d3 = policy.evaluate("c1");
    EXPECT_TRUE(d3.changed);
    EXPECT_EQ(d3.new_tier, nimbus::ChunkTier::COLD);
}

TEST(PolicyTest, NoFlapUnderAlternatingBursts) {
    nimbus::AdaptivePolicy policy(make_cfg());
    for (int i = 0; i < 10; ++i) {
        policy.record_accesses("c1", (i % 2 == 0) ? 100 : 0, 1000);
        auto d = policy.evaluate("c1");
        EXPECT_FALSE(d.changed) << "unexpected tier change at window " << i;
    }
}

TEST(PolicyTest, WarmChunkStaysWarmRf) {
    nimbus::AdaptivePolicy policy(make_cfg());
    policy.record_accesses("c1", 1, 2000); 
    auto dec = policy.evaluate("c1");
    EXPECT_EQ(dec.new_rf, 3);
    EXPECT_EQ(dec.new_tier, nimbus::ChunkTier::WARM);
}

TEST(PolicyTest, GetRateReturnsZeroForUnknownChunk) {
    nimbus::AdaptivePolicy policy(make_cfg());
    EXPECT_DOUBLE_EQ(policy.get_rate("unknown"), 0.0);
}
