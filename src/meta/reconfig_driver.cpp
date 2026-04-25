#include "reconfig_driver.h"
#include "storage.grpc.pb.h"
#include <grpcpp/create_channel.h>
#include <spdlog/spdlog.h>
#include <chrono>

namespace nimbus {

static std::string encode_reconfig_start(const std::string& chunk_id,
                                          const std::vector<std::string>& old_set,
                                          const std::vector<std::string>& new_set) {
    std::string p = chunk_id + "|";
    for (auto& n : old_set) p += n + ",";
    p += "|";
    for (auto& n : new_set) p += n + ",";
    return p;
}

static std::string encode_reconfig_commit(const std::string& chunk_id,
                                           const std::vector<std::string>& new_set) {
    std::string p = chunk_id + "|";
    for (auto& n : new_set) p += n + ",";
    return p;
}

ReconfigDriver::ReconfigDriver(MetaCoordinator& coord,
                                MetaReplication& repl,
                                const Placement&  placement)
    : coord_(coord), repl_(repl), placement_(placement) {}

static std::string resolve_address(const std::string& entry,
                                    const std::vector<NodeEntry>& alive) {
    const size_t eq = entry.find('=');
    if (eq != std::string::npos) return entry.substr(eq + 1);
    for (const auto& n : alive) {
        if (n.node_id == entry) return n.address;
    }
    return {};
}

static std::string encode_entry(const std::string& node_id,
                                 const std::vector<NodeEntry>& alive) {
    for (const auto& n : alive) {
        if (n.node_id == node_id) return node_id + "=" + n.address;
    }
    return node_id;
}

bool ReconfigDriver::reconfig(const std::string& chunk_id, int new_rf) {
    const ChunkEntry* entry = coord_.get_chunk(chunk_id);
    if (!entry) {
        spdlog::error("ReconfigDriver: chunk {} not found", chunk_id);
        return false;
    }
    if (entry->config_state == ChunkConfigState::TRANSITIONING) {
        spdlog::warn("ReconfigDriver: chunk {} already TRANSITIONING", chunk_id);
        return false;
    }

    std::vector<std::string> old_set = entry->replica_set;
    uint64_t current_version = entry->version;

    // Exclude nodes already in old_set. old_set entries may be "node_id=address";
    // extract node_id for the exclude set.
    std::unordered_set<std::string> exclude;
    for (const auto& e : old_set) {
        const size_t eq = e.find('=');
        exclude.insert(eq != std::string::npos ? e.substr(0, eq) : e);
    }

    auto alive = coord_.get_alive_nodes();
    auto new_node_ids = placement_.select_nodes(new_rf, alive, exclude, chunk_id);

    if (static_cast<int>(new_node_ids.size()) < new_rf) {
        spdlog::error("ReconfigDriver: not enough nodes for rf={}", new_rf);
        return false;
    }

    std::vector<std::string> new_nodes;
    new_nodes.reserve(new_node_ids.size());
    for (const auto& nid : new_node_ids)
        new_nodes.push_back(encode_entry(nid, alive));

    spdlog::info("ReconfigDriver: START chunk={} old_rf={} new_rf={}",
                 chunk_id, old_set.size(), new_rf);

    // Step 1: commit RECONFIGURE_START via quorum.
    std::string start_payload = encode_reconfig_start(chunk_id, old_set, new_nodes);
    auto result = repl_.write(WalEntryType::RECONFIGURE_START, start_payload);
    if (result != WriteResult::OK) {
        spdlog::error("ReconfigDriver: quorum write RECONFIGURE_START failed");
        return false;
    }

    // Step 2: coordinator marks chunk TRANSITIONING via the on_commit callback wired
    // in meta_server.cpp → apply_wal_entry → coord.apply_reconfig_start().

    // Step 3: copy chunk data to new replicas.
    if (!copy_to_new_replicas(chunk_id, current_version, old_set, new_nodes)) {
        spdlog::error("ReconfigDriver: data copy to new replicas failed for {}", chunk_id);
        return false;
    }

    // Step 4: verify all new replicas are up to date.
    if (!verify_replicas(chunk_id, current_version, new_nodes)) {
        spdlog::error("ReconfigDriver: replica verification failed for {}", chunk_id);
        return false;
    }

    // Step 5: commit RECONFIGURE_COMMIT via quorum.
    std::string commit_payload = encode_reconfig_commit(chunk_id, new_nodes);
    result = repl_.write(WalEntryType::RECONFIGURE_COMMIT, commit_payload);
    if (result != WriteResult::OK) {
        spdlog::error("ReconfigDriver: quorum write RECONFIGURE_COMMIT failed");
        return false;
    }

    spdlog::info("ReconfigDriver: COMMIT chunk={} new_rf={} nodes=[{}]",
                 chunk_id, new_rf, [&](){
                     std::string s;
                     for (auto& n : new_nodes) s += n + " ";
                     return s;
                 }());
    return true;
}

bool ReconfigDriver::copy_to_new_replicas(const std::string& chunk_id,
                                           uint64_t version,
                                           const std::vector<std::string>& src_nodes,
                                           const std::vector<std::string>& new_nodes) {
    if (src_nodes.empty()) return true;

    auto alive = coord_.get_alive_nodes();
    const std::string src_addr = resolve_address(src_nodes[0], alive);
    if (src_addr.empty()) {
        spdlog::error("ReconfigDriver: cannot resolve address for source {}", src_nodes[0]);
        return false;
    }

    for (const auto& dst_entry : new_nodes) {
        const std::string dst_addr = resolve_address(dst_entry, alive);
        if (dst_addr.empty()) {
            spdlog::error("ReconfigDriver: no address for dest node {}", dst_entry);
            return false;
        }
        auto src_channel = grpc::CreateChannel(src_addr, grpc::InsecureChannelCredentials());
        auto src_stub = nimbus::storage::StorageNode::NewStub(src_channel);

        nimbus::storage::FetchChunkReq fetch_req;
        fetch_req.set_chunk_id(chunk_id);
        fetch_req.set_version(version);

        grpc::ClientContext ctx;
        auto reader = src_stub->FetchChunk(&ctx, fetch_req);

        std::string assembled_data;
        nimbus::storage::ChunkData cd;
        while (reader->Read(&cd)) {
            assembled_data += cd.data();
            if (cd.is_last()) break;
        }
        auto fetch_status = reader->Finish();
        if (!fetch_status.ok()) {
            spdlog::error("ReconfigDriver: FetchChunk from {} failed: {}",
                          src_addr, fetch_status.error_message());
            return false;
        }

        auto dst_channel = grpc::CreateChannel(dst_addr, grpc::InsecureChannelCredentials());
        auto dst_stub = nimbus::storage::StorageNode::NewStub(dst_channel);

        nimbus::storage::WriteChunkReq write_req;
        write_req.set_chunk_id(chunk_id);
        write_req.set_version(version);
        write_req.set_data(assembled_data);

        nimbus::storage::WriteChunkResp write_resp;
        grpc::ClientContext wctx;
        auto ws = dst_stub->WriteChunk(&wctx, write_req, &write_resp);
        if (!ws.ok() || !write_resp.ok()) {
            spdlog::error("ReconfigDriver: WriteChunk to {} failed", dst_addr);
            return false;
        }
        spdlog::debug("ReconfigDriver: copied chunk={} v={} → {}", chunk_id, version, dst_addr);
    }
    return true;
}

bool ReconfigDriver::verify_replicas(const std::string& chunk_id,
                                      uint64_t required_version,
                                      const std::vector<std::string>& nodes) {
    auto alive = coord_.get_alive_nodes();
    for (const auto& entry : nodes) {
        const std::string addr = resolve_address(entry, alive);
        if (addr.empty()) {
            spdlog::error("ReconfigDriver: verify — no address for {}", entry);
            return false;
        }

        auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        auto stub = nimbus::storage::StorageNode::NewStub(channel);

        nimbus::storage::ReadChunkReq req;
        req.set_chunk_id(chunk_id);
        req.set_min_version(required_version);

        nimbus::storage::ReadChunkResp resp;
        grpc::ClientContext ctx;
        auto s = stub->ReadChunk(&ctx, req, &resp);
        if (!s.ok() || !resp.ok() || resp.version() < required_version) {
            spdlog::error("ReconfigDriver: verify failed on {} chunk={}", entry, chunk_id);
            return false;
        }
    }
    return true;
}

} // namespace nimbus
