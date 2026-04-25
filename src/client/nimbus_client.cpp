#include "client/nimbus_client.h"
#include <grpcpp/create_channel.h>
#include <chrono>
#include <iostream>
#include <thread>

namespace nimbus {

static std::pair<std::string, std::string> parse_replica_entry(const std::string& entry) {
    auto eq = entry.find('=');
    if (eq != std::string::npos) {
        return {entry.substr(0, eq), entry.substr(eq + 1)};
    }
    return {entry, ""};
}

NimbusClient::NimbusClient(const std::string& meta_addr,
                           const std::map<std::string, std::string>& node_addrs)
    : node_addrs_(node_addrs) {
    meta_channel_ = grpc::CreateChannel(meta_addr, grpc::InsecureChannelCredentials());
    meta_stub_ = nimbus::meta::MetaCoordinator::NewStub(meta_channel_);
}

nimbus::storage::StorageNode::Stub* NimbusClient::get_or_create_stub(
        const std::string& node_id, const std::string& addr) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!addr.empty()) node_addrs_[node_id] = addr;
    auto it_addr = node_addrs_.find(node_id);
    if (it_addr == node_addrs_.end()) return nullptr;
    const std::string& resolved = it_addr->second;
    auto it = storage_stubs_.find(node_id);
    if (it == storage_stubs_.end()) {
        auto ch = grpc::CreateChannel(resolved, grpc::InsecureChannelCredentials());
        storage_stubs_[node_id] = nimbus::storage::StorageNode::NewStub(ch);
    }
    return storage_stubs_[node_id].get();
}

PutResult NimbusClient::put(const std::string& chunk_id,
                            const std::string& data,
                            int desired_rf) {
    nimbus::meta::PutChunkReq preq;
    preq.set_chunk_id(chunk_id);
    preq.set_size_bytes(data.size());
    preq.set_desired_rf(desired_rf);

    nimbus::meta::PutChunkResp presp;
    grpc::ClientContext ctx;
    grpc::Status st = meta_stub_->PutChunk(&ctx, preq, &presp);
    if (!st.ok()) return PutResult{false, st.error_message(), 0, {}};
    if (!presp.ok()) return PutResult{false, presp.error(), presp.version(), {}};

    uint64_t version = presp.version();

    struct Target {
        std::string node_id;
        std::string addr;
        bool ok = false;
    };
    std::vector<Target> targets;
    targets.reserve(presp.replica_set_size());
    for (const auto& entry : presp.replica_set()) {
        auto [nid, addr] = parse_replica_entry(entry);
        targets.push_back({nid, addr, false});
    }

    auto try_write = [&](Target& t) {
        auto* stub = get_or_create_stub(t.node_id, t.addr);
        if (!stub) return;
        nimbus::storage::WriteChunkReq wreq;
        wreq.set_chunk_id(chunk_id);
        wreq.set_version(version);
        wreq.set_data(data);
        nimbus::storage::WriteChunkResp wresp;
        grpc::ClientContext wctx;
        auto wst = stub->WriteChunk(&wctx, wreq, &wresp);
        t.ok = wst.ok() && wresp.ok();
    };

    for (auto& t : targets) try_write(t);

    // Retry each failed node up to 2 more times before giving up.
    constexpr int kMaxRetries = 2;
    for (int retry = 0; retry < kMaxRetries; ++retry) {
        bool any_failed = false;
        for (const auto& t : targets) if (!t.ok) { any_failed = true; break; }
        if (!any_failed) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        for (auto& t : targets) if (!t.ok) try_write(t);
    }

    std::vector<std::string> replica_nodes;
    std::string failed_ids;
    for (auto& t : targets) {
        if (t.ok) replica_nodes.push_back(t.addr.empty() ? t.node_id : t.addr);
        else      failed_ids += t.node_id + ";";
    }

    if (replica_nodes.empty())
        return PutResult{false, "all replicas failed: " + failed_ids, version, {}};

    // Partial write is ok — replica_repair heals under-replicated chunks.
    return PutResult{true,
                     failed_ids.empty() ? "" : "partial:" + failed_ids,
                     version, replica_nodes};
}

std::vector<std::string> NimbusClient::build_replica_try_list(
        const nimbus::meta::ChunkMeta& meta) {
    std::vector<std::string> list;
    for (const auto& e : meta.replica_set())     list.push_back(e);
    for (const auto& e : meta.old_replica_set()) list.push_back(e);
    return list;
}

GetResult NimbusClient::get(const std::string& chunk_id, uint64_t min_version) {
    nimbus::meta::ChunkInfoReq req;
    req.set_chunk_id(chunk_id);
    nimbus::meta::ChunkInfoResp resp;
    grpc::ClientContext ctx;
    grpc::Status st = meta_stub_->GetChunkInfo(&ctx, req, &resp);
    if (!st.ok()) return GetResult{false, st.error_message(), {}, 0};
    if (!resp.ok()) return GetResult{false, resp.error(), {}, 0};

    uint64_t target_version = (min_version == 0) ? resp.meta().version() : min_version;

    auto try_read = [&](const std::string& entry) -> GetResult {
        auto [node_id, parsed_addr] = parse_replica_entry(entry);
        auto* stub = get_or_create_stub(node_id, parsed_addr);
        if (!stub) return {};
        nimbus::storage::ReadChunkReq rreq;
        rreq.set_chunk_id(chunk_id);
        rreq.set_min_version(target_version);
        nimbus::storage::ReadChunkResp rresp;
        grpc::ClientContext rctx;
        auto rst = stub->ReadChunk(&rctx, rreq, &rresp);
        if (!rst.ok() || !rresp.ok()) return {};
        return GetResult{true, {}, rresp.data(), rresp.version()};
    };

    // Build try-list: current replicas first, old_replica_set as fallback (TRANSITIONING).
    auto replicas = build_replica_try_list(resp.meta());

    constexpr int kAttempts = 60;
    for (int attempt = 0; attempt < kAttempts; ++attempt) {
        for (const auto& entry : replicas) {
            auto result = try_read(entry);
            if (result.ok) return result;
        }
        if (attempt + 1 < kAttempts)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    return GetResult{false, "all replicas failed", {}, 0};
}

bool NimbusClient::remove(const std::string& chunk_id) {
    nimbus::meta::DeleteChunkReq req;
    req.set_chunk_id(chunk_id);
    nimbus::meta::DeleteChunkResp resp;
    grpc::ClientContext ctx;
    auto st = meta_stub_->DeleteChunk(&ctx, req, &resp);
    return st.ok() && resp.ok();
}

} // namespace nimbus
