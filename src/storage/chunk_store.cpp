#include "chunk_store.h"
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>
#include <filesystem>
#include <cstdio>
#include <memory>

namespace nimbus {

namespace {

static std::string make_key(const std::string& id, uint64_t ver) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%010llu", static_cast<unsigned long long>(ver));
    return "chunk/" + id + "/" + buf;
}

static std::string chunk_prefix(const std::string& id) {
    return "chunk/" + id + "/";
}

} // namespace

struct ChunkStore::Impl {
    std::unique_ptr<rocksdb::DB> db;
};

ChunkStore::ChunkStore(const std::string& data_dir) : impl_(new Impl()) {
    std::filesystem::create_directories(data_dir);

    rocksdb::Options opts;
    opts.create_if_missing = true;

    rocksdb::DB* raw = nullptr;
    auto s = rocksdb::DB::Open(opts, data_dir, &raw);
    if (s.ok()) {
        impl_->db.reset(raw);
    }
}

ChunkStore::~ChunkStore() {
    delete impl_;
}

bool ChunkStore::write(const std::string& chunk_id, uint64_t version, const std::string& data) {
    if (!impl_->db) return false;
    rocksdb::WriteOptions wo;
    wo.sync = true;
    auto s = impl_->db->Put(wo, make_key(chunk_id, version), data);
    return s.ok();
}

bool ChunkStore::read(const std::string& chunk_id, uint64_t min_version,
                     std::string& data_out, uint64_t& version_out) {
    if (!impl_->db) return false;

    const std::string prefix = chunk_prefix(chunk_id);
    const std::string seek_key = make_key(chunk_id, 9999999999ULL);

    rocksdb::ReadOptions ro;
    std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(ro));
    it->SeekForPrev(seek_key);

    if (!it->Valid()) return false;

    std::string key = it->key().ToString();
    if (key.rfind(prefix, 0) != 0) return false;

    const std::string ver_str = key.substr(prefix.size());
    uint64_t found_ver = 0;
    try {
        found_ver = static_cast<uint64_t>(std::stoull(ver_str));
    } catch (...) {
        return false;
    }

    if (found_ver < min_version) return false;

    data_out = it->value().ToString();
    version_out = found_ver;
    return true;
}

bool ChunkStore::remove(const std::string& chunk_id) {
    if (!impl_->db) return false;

    const std::string start = make_key(chunk_id, 0);
    const std::string end = chunk_prefix(chunk_id) + "~";

    rocksdb::WriteBatch batch;
    batch.DeleteRange(start, end);

    rocksdb::WriteOptions wo;
    wo.sync = true;
    auto s = impl_->db->Write(wo, &batch);
    return s.ok();
}

uint64_t ChunkStore::latest_version(const std::string& chunk_id) {
    if (!impl_->db) return 0;

    const std::string prefix = chunk_prefix(chunk_id);
    const std::string seek_key = make_key(chunk_id, 9999999999ULL);

    rocksdb::ReadOptions ro;
    std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(ro));
    it->SeekForPrev(seek_key);

    if (!it->Valid()) return 0;

    std::string key = it->key().ToString();
    if (key.rfind(prefix, 0) != 0) return 0;

    const std::string ver_str = key.substr(prefix.size());
    try {
        return static_cast<uint64_t>(std::stoull(ver_str));
    } catch (...) {
        return 0;
    }
}

} // namespace nimbus
