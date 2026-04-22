#pragma once
#include "chunk_store.h"
#include "storage.grpc.pb.h"
#include <grpcpp/grpcpp.h>

namespace nimbus {

class StorageRpcService final : public nimbus::storage::StorageNode::Service {
public:
    explicit StorageRpcService(ChunkStore& store);

    grpc::Status WriteChunk(grpc::ServerContext* ctx,
                            const nimbus::storage::WriteChunkReq* req,
                            nimbus::storage::WriteChunkResp* resp) override;

    grpc::Status ReadChunk(grpc::ServerContext* ctx,
                           const nimbus::storage::ReadChunkReq* req,
                           nimbus::storage::ReadChunkResp* resp) override;

    grpc::Status FetchChunk(grpc::ServerContext* ctx,
                            const nimbus::storage::FetchChunkReq* req,
                            grpc::ServerWriter<nimbus::storage::ChunkData>* writer) override;

    grpc::Status DeleteChunkData(grpc::ServerContext* ctx,
                                 const nimbus::storage::DeleteDataReq* req,
                                 nimbus::storage::DeleteDataResp* resp) override;

private:
    ChunkStore& store_;
};

} // namespace nimbus
