#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include "../../src/client/history_tracer.h"

namespace fs = std::filesystem;

class HistoryTracerTest : public ::testing::Test {
protected:
    std::string path_;
    void SetUp() override {
        path_ = "/tmp/nimbus_history_tracer_test_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        // ensure removed
        fs::remove(path_);
    }
    void TearDown() override {
        fs::remove(path_);
    }
};

TEST_F(HistoryTracerTest, RecordWritesToCsv) {
    {
        nimbus::HistoryTracer tracer(path_);
        tracer.record(nimbus::OpType::PUT, "chunk_1", 1713650000000000ull, 1243, true, 4096);
    }
    std::ifstream ifs(path_);
    ASSERT_TRUE(ifs.is_open());
    std::string line;
    int lines = 0;
    while (std::getline(ifs, line)) ++lines;
    EXPECT_EQ(lines, 2); // header + 1 row
}

TEST_F(HistoryTracerTest, SummaryCountsOps) {
    nimbus::HistoryTracer tracer(path_);
    for (int i = 0; i < 10; ++i) {
        tracer.record(nimbus::OpType::GET, "c", 1000 + i, 10, true, 1024);
    }

    std::ostringstream oss;
    auto* old = std::cout.rdbuf(oss.rdbuf());
    tracer.print_summary();
    std::cout.rdbuf(old);
    std::string out = oss.str();

    auto pos = out.find("Total ops:");
    ASSERT_NE(pos, std::string::npos);
    std::istringstream ss(out.substr(pos));
    std::string tmp;
    ss >> tmp >> tmp; // "Total" "ops:"
    int total = 0;
    ss >> total;
    EXPECT_EQ(total, 10);
}

TEST_F(HistoryTracerTest, SummaryErrorRate) {
    nimbus::HistoryTracer tracer(path_);
    for (int i = 0; i < 8; ++i) tracer.record(nimbus::OpType::PUT, "c", 1000 + i, 5, true, 512);
    for (int i = 0; i < 2; ++i) tracer.record(nimbus::OpType::PUT, "c", 2000 + i, 5, false, 0);

    std::ostringstream oss;
    auto* old = std::cout.rdbuf(oss.rdbuf());
    tracer.print_summary();
    std::cout.rdbuf(old);
    std::string out = oss.str();

    auto pos = out.find("Errors:");
    ASSERT_NE(pos, std::string::npos);
    // find '(' and '%'
    auto p1 = out.find('(', pos);
    auto p2 = out.find('%', p1);
    ASSERT_NE(p1, std::string::npos);
    ASSERT_NE(p2, std::string::npos);
    std::string perc = out.substr(p1+1, p2 - (p1+1));
    double val = std::stod(perc);
    EXPECT_NEAR(val, 20.0, 0.01);
}

TEST_F(HistoryTracerTest, LatencyPercentilesCorrect) {
    nimbus::HistoryTracer tracer(path_);
    for (int i = 1; i <= 100; ++i) {
        tracer.record(nimbus::OpType::GET, "c", 1000 + i, i, true, 0);
    }

    std::ostringstream oss;
    auto* old = std::cout.rdbuf(oss.rdbuf());
    tracer.print_summary();
    std::cout.rdbuf(old);
    std::string out = oss.str();

    auto pos = out.find("Latency (us): p50=");
    ASSERT_NE(pos, std::string::npos);
    auto start = pos + strlen("Latency (us): p50=");
    auto end = out.find(" ", start);
    std::string p50s = out.substr(start, end - start);
    int p50 = std::stoi(p50s);
    EXPECT_EQ(p50, 50);
}

TEST_F(HistoryTracerTest, MultipleOpTypesTracked) {
    nimbus::HistoryTracer tracer(path_);
    for (int i = 0; i < 5; ++i) tracer.record(nimbus::OpType::PUT, "c", 1000 + i, 10, true, 1024);
    for (int i = 0; i < 5; ++i) tracer.record(nimbus::OpType::GET, "c", 2000 + i, 5, true, 512);

    std::ostringstream oss;
    auto* old = std::cout.rdbuf(oss.rdbuf());
    tracer.print_summary();
    std::cout.rdbuf(old);
    std::string out = oss.str();

    EXPECT_NE(out.find("PUT ops/sec"), std::string::npos);
    EXPECT_NE(out.find("GET ops/sec"), std::string::npos);
}
