#include "client/nimbus_client.h"
#include <grpcpp/create_channel.h>
#include <iostream>

namespace nimbus {

NimbusClient::NimbusClient(const std::string& meta_addr,
                           const std::map<std::string, std::string>& node_addrs)
    : node_addrs_(node_addrs) {
    meta_channel_ = grpc::CreateChannel(meta_addr, grpc::InsecureChannelCredentials());
    meta_stub_ = nimbus::meta::MetaCoordinator::NewStub(meta_channel_);
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
    if (!st.ok()) {
        return PutResult{false, st.error_message(), 0, {}};
    }
    if (!presp.ok()) {
        return PutResult{false, presp.error(), presp.version(), {}};
    }

    uint64_t version = presp.version();
    std::vector<std::string> replica_nodes;
    bool all_ok = true;
    std::string all_errs;

    for (const auto& node_id : presp.replica_set()) {
        auto it_addr = node_addrs_.find(node_id);
        if (it_addr == node_addrs_.end()) {
            all_ok = false;
            all_errs += "unknown node_id:" + node_id + ";";
            continue;
        }
        const std::string addr = it_addr->second;

        // ensure we have a stub (create if necessary)
        nimbus::storage::StorageNode::Stub* stub_raw = nullptr;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = storage_stubs_.find(node_id);
            if (it == storage_stubs_.end()) {
                auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
                auto stub = nimbus::storage::StorageNode::NewStub(channel);
                storage_stubs_[node_id] = std::move(stub);
                stub_raw = storage_stubs_[node_id].get();
            } else {
                stub_raw = it->second.get();
            }
        }

        if (!stub_raw) {
            all_ok = false;
            all_errs += "failed_create_stub:" + node_id + ";";
            continue;
        }

        nimbus::storage::WriteChunkReq wreq;
        wreq.set_chunk_id(chunk_id);
        wreq.set_version(version);
        wreq.set_data(data);

        nimbus::storage::WriteChunkResp wresp;
        grpc::ClientContext wctx;
        grpc::Status wst = stub_raw->WriteChunk(&wctx, wreq, &wresp);
        if (!wst.ok() || !wresp.ok()) {
            all_ok = false;
            std::string err = wst.ok() ? wresp.error() : wst.error_message();
            all_errs += node_id + ":" + err + ";";
        } else {
            replica_nodes.push_back(addr);
        }
    }

    if (!all_ok) {
        return PutResult{false, all_errs, version, replica_nodes};
    }
    return PutResult{true, std::string(), version, replica_nodes};
}

GetResult NimbusClient::get(const std::string& chunk_id, uint64_t min_version) {
    nimbus::meta::ChunkInfoReq req;
    req.set_chunk_id(chunk_id);
    nimbus::meta::ChunkInfoResp resp;
    grpc::ClientContext ctx;
    grpc::Status st = meta_stub_->GetChunkInfo(&ctx, req, &resp);
    if (!st.ok()) {
        return GetResult{false, st.error_message(), std::string(), 0};
    }
    if (!resp.ok()) {
        return GetResult{false, resp.error(), std::string(), 0};
    }

    uint64_t version = resp.meta().version();
    for (const auto& node_id : resp.meta().replica_set()) {
        auto it_addr = node_addrs_.find(node_id);
        if (it_addr == node_addrs_.end()) continue;
        const std::string addr = it_addr->second;

        nimbus::storage::StorageNode::Stub* stub_raw = nullptr;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = storage_stubs_.find(node_id);
            if (it == storage_stubs_.end()) {
                auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
                auto stub = nimbus::storage::StorageNode::NewStub(channel);
                storage_stubs_[node_id] = std::move(stub);
                stub_raw = storage_stubs_[node_id].get();
            } else {
                stub_raw = it->second.get();
            }
        }
        if (!stub_raw) continue;

        nimbus::storage::ReadChunkReq rreq;
        rreq.set_chunk_id(chunk_id);
        rreq.set_min_version(min_version == 0 ? version : min_version);

        nimbus::storage::ReadChunkResp rresp;
        grpc::ClientContext rctx;
        grpc::Status rst = stub_raw->ReadChunk(&rctx, rreq, &rresp);
        if (!rst.ok() || !rresp.ok()) {
            continue; // try next replica
        }
        return GetResult{true, std::string(), rresp.data(), rresp.version()};
    }

    return GetResult{false, std::string("all replicas failed"), std::string(), 0};
}

bool NimbusClient::remove(const std::string& chunk_id) {
    nimbus::meta::DeleteChunkReq req;
    req.set_chunk_id(chunk_id);
    nimbus::meta::DeleteChunkResp resp;
    grpc::ClientContext ctx;
    grpc::Status st = meta_stub_->DeleteChunk(&ctx, req, &resp);
    if (!st.ok()) return false;
    return resp.ok();
}

} // namespace nimbus
