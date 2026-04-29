#include "meta_rpc.h"
#include "placement.h"
#include "../common/clock.h"
#include <spdlog/spdlog.h>
#include <grpcpp/create_channel.h>
#include "storage.grpc.pb.h"

static std::string strip_node_id(const std::string& entry) {
    const size_t eq = entry.find('=');
    return (eq != std::string::npos) ? entry.substr(0, eq) : entry;
}

static std::string resolve_replica_address(const nimbus::MetaCoordinator& coord,
                                           const std::string& entry) {
    const size_t eq = entry.find('=');
    if (eq != std::string::npos) return entry.substr(eq + 1);
    for (const auto& n : coord.get_alive_nodes()) {
        if (n.node_id == entry) return n.address;
    }
    return {};
}

static bool write_chunk_to_replica(const std::string& addr,
                                   const std::string& chunk_id,
                                   uint64_t version,
                                   const std::string& data) {
    if (addr.empty()) return false;
    auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    auto stub = nimbus::storage::StorageNode::NewStub(channel);

    nimbus::storage::WriteChunkReq req;
    req.set_chunk_id(chunk_id);
    req.set_version(version);
    req.set_data(data);

    nimbus::storage::WriteChunkResp resp;
    grpc::ClientContext ctx;
    auto st = stub->WriteChunk(&ctx, req, &resp);
    return st.ok() && resp.ok();
}

static void delete_chunk_from_replica(const std::string& addr,
                                     const std::string& chunk_id) {
    if (addr.empty()) return;
    auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    auto stub = nimbus::storage::StorageNode::NewStub(channel);

    nimbus::storage::DeleteDataReq req;
    req.set_chunk_id(chunk_id);
    nimbus::storage::DeleteDataResp resp;
    grpc::ClientContext ctx;
    (void)stub->DeleteChunkData(&ctx, req, &resp);
}

namespace nimbus {

static std::string encode_replica_target(const MetaCoordinator& coord,
                                          const std::string& node_id_or_entry) {
    if (node_id_or_entry.find('=') != std::string::npos)
        return node_id_or_entry;
    for (const auto& n : coord.get_alive_nodes()) {
        if (n.node_id == node_id_or_entry)
            return node_id_or_entry + "=" + n.address;
    }
    return node_id_or_entry; // no address known yet; caller uses node_id alone
}

static std::string encode_heartbeat_payload(const nimbus::meta::HeartbeatReq& req) {
    return req.node_id() + "|" +
           req.address() + "|" +
           std::to_string(req.load_fraction()) + "|" +
           std::to_string(req.free_bytes()) + "|" +
           std::to_string(req.total_bytes()) + "|" +
           std::to_string(req.failure_rate_7d());
}

static std::string extract_node_id(const std::string& entry) {
    const size_t eq = entry.find('=');
    return (eq != std::string::npos) ? entry.substr(0, eq) : entry;
}

static bool has_replica_node(const ChunkEntry& entry, const std::string& node_id) {
    for (const auto& e : entry.replica_set) {
        if (extract_node_id(e) == node_id) return true;
    }
    return false;
}

MetaRpcService::MetaRpcService(MetaCoordinator& coord,
                                MetaReplication* repl,
                                const NodeConfig& cfg,
                                AdaptivePolicy* policy,
                                ReconfigDriver* reconfig)
    : coord_(coord), cfg_(cfg) {
    repl_.store(repl);
    policy_.store(policy);
    reconfig_.store(reconfig);
}

void MetaRpcService::promote(MetaReplication* repl, AdaptivePolicy* policy,
                              ReconfigDriver* reconfig) {
    // Store in dependency order: repl last so callers see consistent state.
    policy_.store(policy);
    reconfig_.store(reconfig);
    repl_.store(repl);
    spdlog::info("MetaRpcService: promoted to leader");
}

grpc::Status MetaRpcService::PutChunk(grpc::ServerContext*,
                                       const nimbus::meta::PutChunkReq* req,
                                       nimbus::meta::PutChunkResp* resp) {
    auto* repl = repl_.load();
    if (!repl) {
        resp->set_ok(false);
        resp->set_error("NotLeader");
        return grpc::Status::OK;
    }

    const ChunkEntry* existing = coord_.get_chunk(req->chunk_id());
    if (existing && existing->config_state == ChunkConfigState::TRANSITIONING) {
        resp->set_ok(false);
        resp->set_error("ChunkTransitioning");
        return grpc::Status::OK;
    }

    PlacementConfig pcfg;
    pcfg.random_placement = (cfg_.mode == "baseline");
    Placement placement(pcfg);
    auto alive = coord_.get_alive_nodes();
    auto nodes = placement.select_nodes(req->desired_rf(), alive, {}, req->chunk_id());

    if (static_cast<int>(nodes.size()) < req->desired_rf()) {
        resp->set_ok(false);
        resp->set_error("not enough nodes");
        return grpc::Status::OK;
    }

    uint64_t version = coord_.next_version(req->chunk_id());

    const std::string data = req->data();
    std::vector<std::string> replica_entries;
    replica_entries.reserve(nodes.size());
    std::vector<std::string> successful_addrs;
    std::string failed_ids;

    for (const auto& n : nodes) {
        const std::string entry = encode_replica_target(coord_, n);
        const std::string addr = resolve_replica_address(coord_, entry);
        replica_entries.push_back(entry);

        if (write_chunk_to_replica(addr, req->chunk_id(), version, data)) {
            successful_addrs.push_back(addr);
        } else {
            failed_ids += strip_node_id(entry) + ";";
        }
    }

    const size_t majority_needed = (replica_entries.size() / 2) + 1;
    if (successful_addrs.size() < majority_needed) {
        for (const auto& addr : successful_addrs) {
            delete_chunk_from_replica(addr, req->chunk_id());
        }
        resp->set_ok(false);
        resp->set_error(failed_ids.empty() ? "storage majority unavailable"
                                           : "storage majority unavailable: " + failed_ids);
        return grpc::Status::OK;
    }

    std::string payload = req->chunk_id() + "|" +
                          std::to_string(req->size_bytes()) + "|" +
                          std::to_string(req->desired_rf()) + "|" +
                          std::to_string(version) + "|";
    for (auto& n : replica_entries) payload += n + ",";

    auto result = repl->write(WalEntryType::PUT_CHUNK, payload);
    if (result != WriteResult::OK) {
        for (const auto& addr : successful_addrs) {
            delete_chunk_from_replica(addr, req->chunk_id());
        }
        resp->set_ok(false);
        resp->set_error("metadata quorum unavailable after storage write");
        return grpc::Status::OK;
    }

    resp->set_ok(true);
    resp->set_version(version);
    for (auto& n : replica_entries) resp->add_replica_set(n);
    return grpc::Status::OK;
}

grpc::Status MetaRpcService::GetChunkInfo(grpc::ServerContext*,
                                           const nimbus::meta::ChunkInfoReq* req,
                                           nimbus::meta::ChunkInfoResp* resp) {
    if (!repl_.load()) {
        resp->set_ok(false);
        resp->set_error("NotLeader");
        return grpc::Status::OK;
    }

    auto* entry = coord_.get_chunk(req->chunk_id());
    if (!entry) {
        resp->set_ok(false);
        resp->set_error("not found");
        return grpc::Status::OK;
    }
    resp->set_ok(true);
    auto* m = resp->mutable_meta();
    m->set_chunk_id(entry->chunk_id);
    m->set_version(entry->version);
    m->set_replication_factor(entry->replication_factor);
    m->set_size_bytes(entry->size_bytes);
    m->set_config_state(entry->config_state == ChunkConfigState::STABLE
                        ? nimbus::meta::ConfigState::STABLE
                        : nimbus::meta::ConfigState::TRANSITIONING);
    for (auto& n : entry->replica_set) {
        m->add_replica_set(encode_replica_target(coord_, n));
    }
    for (auto& n : entry->old_replica_set) {
        m->add_old_replica_set(encode_replica_target(coord_, n));
    }
    return grpc::Status::OK;
}

grpc::Status MetaRpcService::DeleteChunk(grpc::ServerContext*,
                                          const nimbus::meta::DeleteChunkReq* req,
                                          nimbus::meta::DeleteChunkResp* resp) {
    auto* repl = repl_.load();
    if (!repl) {
        resp->set_ok(false);
        resp->set_error("NotLeader");
        return grpc::Status::OK;
    }
    auto result = repl->write(WalEntryType::DELETE_CHUNK, req->chunk_id());
    resp->set_ok(result == WriteResult::OK);
    if (result != WriteResult::OK) resp->set_error("quorum unavailable");
    return grpc::Status::OK;
}

grpc::Status MetaRpcService::AddReplica(grpc::ServerContext*,
                                         const nimbus::meta::ReplicaChangeReq* req,
                                         nimbus::meta::ReplicaChangeResp* resp) {
    auto* reconfig = reconfig_.load();
    if (!repl_.load() || !reconfig) {
        resp->set_ok(false);
        resp->set_error("NotLeader");
        return grpc::Status::OK;
    }
    const ChunkEntry* entry = coord_.get_chunk(req->chunk_id());
    if (!entry) {
        resp->set_ok(false);
        resp->set_error("chunk not found");
        return grpc::Status::OK;
    }
    if (req->node_id().empty()) {
        resp->set_ok(false);
        resp->set_error("node_id required");
        return grpc::Status::OK;
    }
    if (has_replica_node(*entry, req->node_id())) {
        resp->set_ok(false);
        resp->set_error("node already in replica set");
        return grpc::Status::OK;
    }
    int new_rf = static_cast<int>(entry->replica_set.size()) + 1;
    bool ok = reconfig->reconfig(req->chunk_id(), new_rf, req->node_id(), "");
    resp->set_ok(ok);
    if (!ok) resp->set_error("reconfig failed");
    return grpc::Status::OK;
}

grpc::Status MetaRpcService::RemoveReplica(grpc::ServerContext*,
                                            const nimbus::meta::ReplicaChangeReq* req,
                                            nimbus::meta::ReplicaChangeResp* resp) {
    auto* reconfig = reconfig_.load();
    if (!repl_.load() || !reconfig) {
        resp->set_ok(false);
        resp->set_error("NotLeader");
        return grpc::Status::OK;
    }
    const ChunkEntry* entry = coord_.get_chunk(req->chunk_id());
    if (!entry || entry->replica_set.size() <= 1) {
        resp->set_ok(false);
        resp->set_error(entry ? "cannot remove last replica" : "chunk not found");
        return grpc::Status::OK;
    }
    if (req->node_id().empty()) {
        resp->set_ok(false);
        resp->set_error("node_id required");
        return grpc::Status::OK;
    }
    if (!has_replica_node(*entry, req->node_id())) {
        resp->set_ok(false);
        resp->set_error("node not in replica set");
        return grpc::Status::OK;
    }
    int new_rf = static_cast<int>(entry->replica_set.size()) - 1;
    bool ok = reconfig->reconfig(req->chunk_id(), new_rf, "", req->node_id());
    resp->set_ok(ok);
    if (!ok) resp->set_error("reconfig failed");
    return grpc::Status::OK;
}

grpc::Status MetaRpcService::NodeHeartbeat(grpc::ServerContext*,
                                            const nimbus::meta::HeartbeatReq* req,
                                            nimbus::meta::HeartbeatResp* resp) {
    auto* repl   = repl_.load();
    auto* policy = policy_.load();
    if (!repl) {
        resp->set_ok(false);
        if (!cfg_.peers.empty()) resp->set_leader_hint(cfg_.peers.front());
        return grpc::Status::OK;
    }

    const std::string payload = encode_heartbeat_payload(*req);
    auto result = repl->write(WalEntryType::NODE_HEARTBEAT, payload);
    if (result != WriteResult::OK) {
        resp->set_ok(false);
        resp->set_leader_hint(cfg_.address);
        return grpc::Status::OK;
    }

    if (policy) {
        const uint64_t window_ms = static_cast<uint64_t>(cfg_.heartbeat_interval_ms);
        for (const auto& [chunk_id, count] : req->chunk_access_counts()) {
            policy->record_accesses(chunk_id, static_cast<uint64_t>(count), window_ms);
        }
    }

    auto all_chunks = coord_.get_chunks();
    for (const auto& c : all_chunks) {
        for (const auto& replica : c.replica_set) {
            const size_t eq = replica.find('=');
            const std::string rid = (eq != std::string::npos) ? replica.substr(0, eq) : replica;
            if (rid == req->node_id()) {
                resp->add_assigned_chunk_ids(c.chunk_id);
                break;
            }
        }
    }

    resp->set_ok(true);
    return grpc::Status::OK;
}


MetaReplService::MetaReplService(MetaFollower& follower,
                                  MetaSnapshot& snapshot,
                                  MetaReplication* repl)
    : follower_(follower), snapshot_(snapshot), repl_(repl) {}

grpc::Status MetaReplService::AppendEntry(grpc::ServerContext*,
                                           const nimbus::repl::AppendEntryReq* req,
                                           nimbus::repl::AppendEntryResp* resp) {
    follower_.record_leader_heartbeat();
    const auto& e = req->entry();
    bool ok = follower_.handle_append(
        req->leader_term(),
        req->prev_log_index(), req->prev_log_term(),
        e.log_index(), e.term(),
        static_cast<WalEntryType>(e.type()),
        e.payload(),
        req->leader_committed_lsn());

    resp->set_success(ok);
    resp->set_term(follower_.current_term());
    return grpc::Status::OK;
}

grpc::Status MetaReplService::CommitEntry(grpc::ServerContext*,
                                           const nimbus::repl::CommitReq* req,
                                           nimbus::repl::CommitResp* resp) {
    follower_.handle_commit(req->log_index(), req->term());
    resp->set_ok(true);
    return grpc::Status::OK;
}

grpc::Status MetaReplService::InstallSnapshot(
        grpc::ServerContext*,
        const nimbus::repl::SnapshotReq* req,
        grpc::ServerWriter<nimbus::repl::SnapshotChunk>* writer) {
    // Leader side: stream our snapshot to a new follower.
    auto [data, lsn] = snapshot_.take_snapshot();

    constexpr size_t CHUNK_SIZE = 65536;
    size_t sent = 0;
    while (sent < data.size()) {
        size_t n = std::min(CHUNK_SIZE, data.size() - sent);
        nimbus::repl::SnapshotChunk chunk;
        chunk.set_data(data.data() + sent, n);
        sent += n;
        chunk.set_is_last_chunk(sent >= data.size());
        chunk.set_snapshot_lsn(lsn);
        writer->Write(chunk);
    }
    (void)req;
    return grpc::Status::OK;
}

grpc::Status MetaReplService::FetchLogEntries(
        grpc::ServerContext*,
        const nimbus::repl::FetchLogReq* req,
        grpc::ServerWriter<nimbus::repl::LogEntry>* writer) {
    const uint64_t from = req->from_index();
    const uint64_t last = repl_ ? repl_->last_log_index() : 0;
    spdlog::info("FetchLogEntries from={} last={}", from, last);

    for (uint64_t idx = from; idx <= last; ++idx) {
        nimbus::WalEntry e;
        if (!follower_.wal().read_entry(idx, e)) break;

        nimbus::repl::LogEntry out;
        out.set_log_index(e.log_index);
        out.set_term(e.term);
        out.set_type(static_cast<nimbus::repl::EntryType>(e.type));
        out.set_payload(e.payload);
        if (!writer->Write(out)) break;
    }

    return grpc::Status::OK;
}

} // namespace nimbus
