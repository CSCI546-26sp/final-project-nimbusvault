#include "meta_rpc.h"
#include "placement.h"
#include "../common/clock.h"
#include <spdlog/spdlog.h>

namespace nimbus {

MetaRpcService::MetaRpcService(MetaCoordinator& coord,
                                MetaReplication* repl,
                                const NodeConfig& cfg,
                                AdaptivePolicy* policy,
                                ReconfigDriver* reconfig)
    : coord_(coord), repl_(repl), cfg_(cfg), policy_(policy), reconfig_(reconfig) {}

grpc::Status MetaRpcService::PutChunk(grpc::ServerContext*,
                                       const nimbus::meta::PutChunkReq* req,
                                       nimbus::meta::PutChunkResp* resp) {
    if (!repl_) {
        resp->set_ok(false);
        resp->set_error("NotLeader");
        return grpc::Status::OK;
    }

    PlacementConfig pcfg;
    Placement placement(pcfg);
    auto alive = coord_.get_alive_nodes();
    auto nodes = placement.select_nodes(req->desired_rf(), alive, {}, req->chunk_id());

    if (static_cast<int>(nodes.size()) < req->desired_rf()) {
        resp->set_ok(false);
        resp->set_error("not enough nodes");
        return grpc::Status::OK;
    }

    uint64_t version = coord_.next_version(req->chunk_id());

    std::string payload = req->chunk_id() + "|" +
                          std::to_string(req->size_bytes()) + "|" +
                          std::to_string(req->desired_rf()) + "|" +
                          std::to_string(version) + "|";
    for (auto& n : nodes) payload += n + ",";

    auto result = repl_->write(WalEntryType::PUT_CHUNK, payload);
    if (result != WriteResult::OK) {
        resp->set_ok(false);
        resp->set_error("quorum unavailable");
        return grpc::Status::OK;
    }

    resp->set_ok(true);
    resp->set_version(version);
    for (auto& n : nodes) resp->add_replica_set(encode_replica_target(coord_, n));
    return grpc::Status::OK;
}

grpc::Status MetaRpcService::GetChunkInfo(grpc::ServerContext*,
                                           const nimbus::meta::ChunkInfoReq* req,
                                           nimbus::meta::ChunkInfoResp* resp) {
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
    if (!repl_) {
        resp->set_ok(false);
        resp->set_error("NotLeader");
        return grpc::Status::OK;
    }
    auto result = repl_->write(WalEntryType::DELETE_CHUNK, req->chunk_id());
    resp->set_ok(result == WriteResult::OK);
    if (result != WriteResult::OK) resp->set_error("quorum unavailable");
    return grpc::Status::OK;
}

grpc::Status MetaRpcService::AddReplica(grpc::ServerContext*,
                                         const nimbus::meta::ReplicaChangeReq* req,
                                         nimbus::meta::ReplicaChangeResp* resp) {
    if (!repl_ || !reconfig_) {
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
    int new_rf = static_cast<int>(entry->replica_set.size()) + 1;
    bool ok = reconfig_->reconfig(req->chunk_id(), new_rf);
    resp->set_ok(ok);
    if (!ok) resp->set_error("reconfig failed");
    return grpc::Status::OK;
}

grpc::Status MetaRpcService::RemoveReplica(grpc::ServerContext*,
                                            const nimbus::meta::ReplicaChangeReq* req,
                                            nimbus::meta::ReplicaChangeResp* resp) {
    if (!repl_ || !reconfig_) {
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
    int new_rf = static_cast<int>(entry->replica_set.size()) - 1;
    bool ok = reconfig_->reconfig(req->chunk_id(), new_rf);
    resp->set_ok(ok);
    if (!ok) resp->set_error("reconfig failed");
    return grpc::Status::OK;
}

grpc::Status MetaRpcService::NodeHeartbeat(grpc::ServerContext*,
                                            const nimbus::meta::HeartbeatReq* req,
                                            nimbus::meta::HeartbeatResp* resp) {
    coord_.apply_heartbeat(req->node_id(), req->address(),
                           req->load_fraction(), req->free_bytes(),
                           req->total_bytes(), req->failure_rate_7d());

    if (policy_) {
        const uint64_t window_ms = static_cast<uint64_t>(cfg_.heartbeat_interval_ms);
        for (const auto& [chunk_id, count] : req->chunk_access_counts()) {
            policy_->record_accesses(chunk_id, static_cast<uint64_t>(count), window_ms);
        }
    }

    // Return the set of chunk IDs assigned to this storage node.
    auto all_chunks = coord_.get_chunks();
    for (const auto& c : all_chunks) {
        for (const auto& replica : c.replica_set) {
            // replica entries are "node_id=address"; extract node_id
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
