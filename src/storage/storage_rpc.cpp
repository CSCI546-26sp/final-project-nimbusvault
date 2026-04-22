#include "storage_rpc.h"
#include "stats_reporter.h"

#include <algorithm>

namespace nimbus {

StorageRpcService::StorageRpcService(ChunkStore& store, StatsReporter& reporter)
    : store_(store), reporter_(reporter) {}

grpc::Status StorageRpcService::WriteChunk(grpc::ServerContext*,
                                           const nimbus::storage::WriteChunkReq* req,
                                           nimbus::storage::WriteChunkResp* resp) {
    bool ok = store_.write(req->chunk_id(), req->version(), req->data());
    resp->set_ok(ok);
    if (!ok) resp->set_error("write failed");
    return grpc::Status::OK;
}

grpc::Status StorageRpcService::ReadChunk(grpc::ServerContext*,
                                          const nimbus::storage::ReadChunkReq* req,
                                          nimbus::storage::ReadChunkResp* resp) {
    std::string data;
    uint64_t version = 0;
    bool ok = store_.read(req->chunk_id(), req->min_version(), data, version);
    if (!ok) {
        resp->set_ok(false);
        resp->set_error("version too old or chunk missing");
        return grpc::Status::OK;
    }

    if (version < req->min_version()) {
        resp->set_ok(false);
        resp->set_error("version too old");
        return grpc::Status::OK;
    }

    resp->set_ok(true);
    resp->set_data(data);
    resp->set_version(version);
    reporter_.record_access(req->chunk_id());
    return grpc::Status::OK;
}

grpc::Status StorageRpcService::FetchChunk(grpc::ServerContext*,
                                           const nimbus::storage::FetchChunkReq* req,
                                           grpc::ServerWriter<nimbus::storage::ChunkData>* writer) {
    std::string data;
    uint64_t version = 0;
    bool ok = store_.read(req->chunk_id(), req->version(), data, version);
    if (!ok || version < req->version()) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "chunk/version not found");
    }

    reporter_.record_access(req->chunk_id());

    constexpr size_t kChunk = 64 * 1024;
    size_t off = 0;
    while (off < data.size()) {
        size_t n = std::min(kChunk, data.size() - off);
        nimbus::storage::ChunkData out;
        out.set_data(data.data() + off, n);
        out.set_version(version);
        off += n;
        out.set_is_last(off >= data.size());
        writer->Write(out);
    }

    if (data.empty()) {
        nimbus::storage::ChunkData out;
        out.set_version(version);
        out.set_is_last(true);
        writer->Write(out);
    }

    return grpc::Status::OK;
}

grpc::Status StorageRpcService::DeleteChunkData(grpc::ServerContext*,
                                                const nimbus::storage::DeleteDataReq* req,
                                                nimbus::storage::DeleteDataResp* resp) {
    bool ok = store_.remove(req->chunk_id());
    resp->set_ok(ok);
    if (!ok) resp->set_error("delete failed");
    return grpc::Status::OK;
}

} // namespace nimbus
