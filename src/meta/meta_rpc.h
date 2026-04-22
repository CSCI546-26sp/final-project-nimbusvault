#pragma once
#include "meta_coordinator.h"
#include "meta_replication.h"
#include "meta_follower.h"
#include "meta_snapshot.h"
#include "meta.grpc.pb.h"
#include "meta_repl.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>

namespace nimbus {

class MetaRpcService final : public nimbus::meta::MetaCoordinator::Service {
public:
    MetaRpcService(MetaCoordinator& coord,
                   MetaReplication* repl,  
                   const NodeConfig& cfg);

    grpc::Status PutChunk(grpc::ServerContext* ctx,
                          const nimbus::meta::PutChunkReq* req,
                          nimbus::meta::PutChunkResp* resp) override;

    grpc::Status GetChunkInfo(grpc::ServerContext* ctx,
                              const nimbus::meta::ChunkInfoReq* req,
                              nimbus::meta::ChunkInfoResp* resp) override;

    grpc::Status DeleteChunk(grpc::ServerContext* ctx,
                             const nimbus::meta::DeleteChunkReq* req,
                             nimbus::meta::DeleteChunkResp* resp) override;

    grpc::Status AddReplica(grpc::ServerContext* ctx,
                            const nimbus::meta::ReplicaChangeReq* req,
                            nimbus::meta::ReplicaChangeResp* resp) override;

    grpc::Status RemoveReplica(grpc::ServerContext* ctx,
                               const nimbus::meta::ReplicaChangeReq* req,
                               nimbus::meta::ReplicaChangeResp* resp) override;

    grpc::Status NodeHeartbeat(grpc::ServerContext* ctx,
                               const nimbus::meta::HeartbeatReq* req,
                               nimbus::meta::HeartbeatResp* resp) override;

private:
    MetaCoordinator& coord_;
    MetaReplication* repl_;   
    NodeConfig       cfg_;
};
class MetaReplService final : public nimbus::repl::MetaReplication::Service {
public:
    MetaReplService(MetaFollower& follower, MetaSnapshot& snapshot,
                    MetaReplication* repl);

    grpc::Status AppendEntry(grpc::ServerContext* ctx,
                             const nimbus::repl::AppendEntryReq* req,
                             nimbus::repl::AppendEntryResp* resp) override;

    grpc::Status CommitEntry(grpc::ServerContext* ctx,
                             const nimbus::repl::CommitReq* req,
                             nimbus::repl::CommitResp* resp) override;

    grpc::Status InstallSnapshot(grpc::ServerContext* ctx,
                                 const nimbus::repl::SnapshotReq* req,
                                 grpc::ServerWriter<nimbus::repl::SnapshotChunk>* writer) override;

    grpc::Status FetchLogEntries(grpc::ServerContext* ctx,
                                  const nimbus::repl::FetchLogReq* req,
                                  grpc::ServerWriter<nimbus::repl::LogEntry>* writer) override;

private:
    MetaFollower&    follower_;
    MetaSnapshot&    snapshot_;
    MetaReplication* repl_;  
};

} // namespace nimbus
