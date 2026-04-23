#include "replica_repair.h"

#include <grpcpp/create_channel.h>
#include <spdlog/spdlog.h>

#include <unordered_set>

namespace nimbus {

ReplicaRepair::ReplicaRepair(const NodeConfig& cfg,
                             const std::string& meta_addr,
                             ChunkStore& store,
                             StatsReporter& reporter)
    : cfg_(cfg), meta_addr_(meta_addr), store_(store), reporter_(reporter) {
    channel_ = grpc::CreateChannel(meta_addr_, grpc::InsecureChannelCredentials());
    meta_stub_ = nimbus::meta::MetaCoordinator::NewStub(channel_);
}

void ReplicaRepair::run_once() {
    if (stopped_.load()) return;
    if (!reporter_.has_assignment_snapshot()) return;

    const std::vector<std::string> assigned_ids = reporter_.assigned_chunk_ids();
    if (assigned_ids.empty()) {
        spdlog::debug("repair: node={} assigned=0; skipping", cfg_.node_id);
        return;
    }

    spdlog::debug("repair: node={} assigned_chunks={}", cfg_.node_id, assigned_ids.size());

    std::unordered_set<std::string> assigned_set(assigned_ids.begin(), assigned_ids.end());

    for (const auto& chunk_id : assigned_set) {
        nimbus::meta::ChunkInfoReq req;
        req.set_chunk_id(chunk_id);

        nimbus::meta::ChunkInfoResp resp;
        grpc::ClientContext ctx;
        grpc::Status st = meta_stub_->GetChunkInfo(&ctx, req, &resp);
        if (!st.ok() || !resp.ok()) continue;

        const uint64_t target_version = resp.meta().version();
        const uint64_t local_version = store_.latest_version(chunk_id);
        if (local_version >= target_version) continue;

        std::string repaired_data;
        uint64_t repaired_version = 0;
        bool repaired = false;

        for (const auto& replica : resp.meta().replica_set()) {
            std::string peer_id;
            std::string peer_addr;
            if (!parse_replica_endpoint(replica, peer_id, peer_addr)) continue;
            if (peer_addr.empty()) continue;
            if (peer_id == cfg_.node_id || peer_addr == cfg_.address) continue;

            if (fetch_from_peer(peer_addr, chunk_id, target_version, repaired_data, repaired_version)) {
                repaired = true;
                break;
            }
        }

        if (repaired) {
            store_.write(chunk_id, repaired_version, repaired_data);
            spdlog::info("repair: chunk={} synced version={}", chunk_id, repaired_version);
        }
    }

    const std::vector<std::string> local_chunks = store_.list_chunk_ids();
    for (const auto& chunk_id : local_chunks) {
        if (assigned_set.find(chunk_id) == assigned_set.end()) {
            store_.remove(chunk_id);
            spdlog::info("repair: chunk={} removed (unassigned)", chunk_id);
        }
    }
}

void ReplicaRepair::stop() {
    stopped_.store(true);
}

bool ReplicaRepair::parse_replica_endpoint(const std::string& entry,
                                           std::string& node_id,
                                           std::string& address) {
    const size_t pos = entry.find('=');
    if (pos == std::string::npos) {
        node_id = entry;
        address.clear();
        return true;
    }

    node_id = entry.substr(0, pos);
    address = entry.substr(pos + 1);
    return !node_id.empty();
}

bool ReplicaRepair::fetch_from_peer(const std::string& peer_addr,
                                    const std::string& chunk_id,
                                    uint64_t target_version,
                                    std::string& data_out,
                                    uint64_t& version_out) {
    nimbus::storage::StorageNode::Stub* stub_raw = nullptr;
    {
        std::lock_guard<std::mutex> lk(stubs_mu_);
        auto it = storage_stubs_.find(peer_addr);
        if (it == storage_stubs_.end()) {
            auto channel = grpc::CreateChannel(peer_addr, grpc::InsecureChannelCredentials());
            storage_stubs_[peer_addr] = nimbus::storage::StorageNode::NewStub(channel);
            stub_raw = storage_stubs_[peer_addr].get();
        } else {
            stub_raw = it->second.get();
        }
    }

    if (!stub_raw) return false;

    nimbus::storage::FetchChunkReq req;
    req.set_chunk_id(chunk_id);
    req.set_version(target_version);

    grpc::ClientContext ctx;
    std::unique_ptr<grpc::ClientReader<nimbus::storage::ChunkData>> reader(
        stub_raw->FetchChunk(&ctx, req));

    std::string data;
    uint64_t streamed_version = 0;
    bool received = false;

    nimbus::storage::ChunkData piece;
    while (reader->Read(&piece)) {
        data.append(piece.data());
        if (piece.version() > streamed_version) {
            streamed_version = piece.version();
        }
        received = true;
        if (piece.is_last()) break;
    }

    grpc::Status st = reader->Finish();
    if (!st.ok() || !received || streamed_version < target_version) {
        return false;
    }

    data_out = std::move(data);
    version_out = streamed_version;
    return true;
}

} // namespace nimbus
