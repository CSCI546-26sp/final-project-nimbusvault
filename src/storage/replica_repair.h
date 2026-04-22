#pragma once

#include "../common/config.h"
#include "chunk_store.h"
#include "meta.grpc.pb.h"
#include "stats_reporter.h"
#include "storage.grpc.pb.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace nimbus {

class ReplicaRepair {
public:
    ReplicaRepair(const NodeConfig& cfg,
                  const std::string& meta_addr,
                  ChunkStore& store,
                  StatsReporter& reporter);

    void run_once();
    void stop();

private:
    static bool parse_replica_endpoint(const std::string& entry,
                                       std::string& node_id,
                                       std::string& address);

    bool fetch_from_peer(const std::string& peer_addr,
                         const std::string& chunk_id,
                         uint64_t target_version,
                         std::string& data_out,
                         uint64_t& version_out);

    NodeConfig cfg_;
    std::string meta_addr_;
    ChunkStore& store_;
    StatsReporter& reporter_;
    std::atomic<bool> stopped_{false};

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<nimbus::meta::MetaCoordinator::Stub> meta_stub_;

    std::mutex stubs_mu_;
    std::unordered_map<std::string, std::unique_ptr<nimbus::storage::StorageNode::Stub>> storage_stubs_;
};

} // namespace nimbus
