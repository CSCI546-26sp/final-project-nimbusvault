#pragma once
#include <string>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <grpcpp/grpcpp.h>
#include "meta.grpc.pb.h"
#include "storage.grpc.pb.h"

namespace nimbus {

struct PutResult {
    bool ok;
    std::string error;
    uint64_t version;
    std::vector<std::string> replica_nodes; // addresses to write data to
};

struct GetResult {
    bool ok;
    std::string error;
    std::string data;
    uint64_t version;
};

class NimbusClient {
public:
    NimbusClient(const std::string& meta_addr,
                 const std::map<std::string, std::string>& node_addrs);

    PutResult put(const std::string& chunk_id,
                  const std::string& data,
                  int desired_rf = 3);

    GetResult get(const std::string& chunk_id,
                  uint64_t min_version = 0);

    bool remove(const std::string& chunk_id);

private:
    std::shared_ptr<grpc::Channel> meta_channel_;
    std::unique_ptr<nimbus::meta::MetaCoordinator::Stub> meta_stub_;

    std::map<std::string, std::string> node_addrs_; // node_id -> address
    std::map<std::string, std::unique_ptr<nimbus::storage::StorageNode::Stub>> storage_stubs_;
    std::mutex mu_;
};

} // namespace nimbus
