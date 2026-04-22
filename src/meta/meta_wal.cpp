#include "meta_wal.h"
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/options.h>
#include <spdlog/spdlog.h>
#include <memory>
#include <stdexcept>
#include <cassert>
#include <cstring>

namespace nimbus {

static std::string wal_key(uint64_t idx) {
    char buf[16];
    std::memcpy(buf, "wal/", 4);
    uint64_t be = __builtin_bswap64(idx);
    std::memcpy(buf + 4, &be, 8);
    return {buf, 12};
}

static std::string cmt_key(uint64_t idx) {
    char buf[16];
    std::memcpy(buf, "cmt/", 4);
    uint64_t be = __builtin_bswap64(idx);
    std::memcpy(buf + 4, &be, 8);
    return {buf, 12};
}

static std::string encode_entry(const WalEntry& e) {
    uint32_t plen = static_cast<uint32_t>(e.payload.size());
    std::string out(1 + 8 + 4 + plen, '\0');
    out[0] = static_cast<uint8_t>(e.type);
    uint64_t term_be = __builtin_bswap64(e.term);
    std::memcpy(&out[1], &term_be, 8);
    uint32_t plen_be = __builtin_bswap32(plen);
    std::memcpy(&out[9], &plen_be, 4);
    std::memcpy(&out[13], e.payload.data(), plen);
    return out;
}

static WalEntry decode_entry(uint64_t idx, const std::string& raw) {
    WalEntry e;
    e.log_index = idx;
    e.type = static_cast<WalEntryType>(static_cast<uint8_t>(raw[0]));
    uint64_t term_be;
    std::memcpy(&term_be, &raw[1], 8);
    e.term = __builtin_bswap64(term_be);
    uint32_t plen_be;
    std::memcpy(&plen_be, &raw[9], 4);
    uint32_t plen = __builtin_bswap32(plen_be);
    e.payload = raw.substr(13, plen);
    return e;
}

struct MetaWal::Impl {
    std::unique_ptr<rocksdb::DB> db;
    uint64_t         last_index_   = 0;
    uint64_t         committed_    = 0;

    void open(const std::string& dir) {
        rocksdb::Options opts;
        opts.create_if_missing = true;
        opts.wal_dir           = dir + "/wal";
        auto status = rocksdb::DB::Open(opts, dir + "/metawal", &db);
        if (!status.ok()) {
            throw std::runtime_error("MetaWal open failed: " + status.ToString());
        }
        std::string val;
        if (db->Get(rocksdb::ReadOptions(), "meta/last_index", &val).ok()) {
            uint64_t be;
            std::memcpy(&be, val.data(), 8);
            last_index_ = __builtin_bswap64(be);
        }
        if (db->Get(rocksdb::ReadOptions(), "meta/committed", &val).ok()) {
            uint64_t be;
            std::memcpy(&be, val.data(), 8);
            committed_ = __builtin_bswap64(be);
        }
    }

    void persist_last_index() {
        uint64_t be = __builtin_bswap64(last_index_);
        std::string v(reinterpret_cast<char*>(&be), 8);
        db->Put(rocksdb::WriteOptions(), "meta/last_index", v);
    }

    void persist_committed() {
        uint64_t be = __builtin_bswap64(committed_);
        std::string v(reinterpret_cast<char*>(&be), 8);
        db->Put(rocksdb::WriteOptions(), "meta/committed", v);
    }
};

MetaWal::MetaWal(const std::string& data_dir) : impl_(new Impl()) {
    impl_->open(data_dir);
    spdlog::info("MetaWal opened: last_index={} committed={}",
                 impl_->last_index_, impl_->committed_);
}

MetaWal::~MetaWal() {
    delete impl_;
}

uint64_t MetaWal::append(WalEntryType type, uint64_t term, const std::string& payload) {
    uint64_t idx = ++impl_->last_index_;
    WalEntry e{idx, term, type, payload};
    rocksdb::WriteOptions wo;
    wo.sync = true;
    auto s = impl_->db->Put(wo, wal_key(idx), encode_entry(e));
    if (!s.ok()) throw std::runtime_error("WAL append failed: " + s.ToString());
    impl_->persist_last_index();
    return idx;
}

void MetaWal::commit(uint64_t log_index) {
    assert(log_index <= impl_->last_index_);
    rocksdb::WriteOptions wo;
    wo.sync = true;
    impl_->db->Put(wo, cmt_key(log_index), "1");
    if (log_index > impl_->committed_) {
        impl_->committed_ = log_index;
        impl_->persist_committed();
    }
}

void MetaWal::replay(const ReplayCallback& cb) {
    for (uint64_t idx = 1; idx <= impl_->committed_; ++idx) {
        std::string raw;
        auto s = impl_->db->Get(rocksdb::ReadOptions(), wal_key(idx), &raw);
        if (!s.ok()) {
            spdlog::warn("WAL replay: missing entry at index {}", idx);
            continue;
        }
        cb(decode_entry(idx, raw));
    }
    spdlog::info("WAL replay complete: {} entries applied", impl_->committed_);
}

uint64_t MetaWal::last_log_index() const  { return impl_->last_index_; }
uint64_t MetaWal::committed_index() const { return impl_->committed_; }

} // namespace nimbus
