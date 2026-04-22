#pragma once
#include "../common/config.h"
#include "meta.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace nimbus {

class StatsReporter {
public:
    StatsReporter(const NodeConfig& cfg, const std::string& meta_addr);

    void record_access(const std::string& chunk_id);
    void run_once();
    void stop();
    std::vector<std::string> assigned_chunk_ids() const;
    bool has_assignment_snapshot() const;

private:
    float compute_load_fraction() const;
    bool compute_disk_usage(uint64_t& free_bytes, uint64_t& total_bytes) const;

    NodeConfig cfg_;
    std::string meta_addr_;
    std::atomic<bool> stopped_{false};

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<nimbus::meta::MetaCoordinator::Stub> stub_;

    mutable std::mutex mu_;
    std::unordered_map<std::string, uint64_t> access_counts_;
    std::vector<std::string> assigned_chunk_ids_;
    bool has_assignment_snapshot_ = false;
};

} // namespace nimbus
