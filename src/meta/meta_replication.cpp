#include "meta_replication.h"
#include "../common/clock.h"
#include "meta_repl.grpc.pb.h"
#include <spdlog/spdlog.h>
#include <grpcpp/create_channel.h>
#include <thread>
#include <chrono>
#include <stdexcept>

namespace nimbus {

MetaReplication::MetaReplication(MetaWal& wal,
                                  const NodeConfig& cfg,
                                  CommitCallback on_commit)
    : wal_(wal), cfg_(cfg), on_commit_(std::move(on_commit)) {}

MetaReplication::~MetaReplication() = default;

void MetaReplication::add_follower(const std::string& address) {
    std::lock_guard<std::mutex> lk(mu_);
    FollowerState fs;
    fs.address      = address;
    fs.next_index   = wal_.last_log_index() + 1;
    fs.last_ack_ms  = now_ms();
    followers_[address] = fs;
    spdlog::info("MetaReplication: added follower {}", address);
}

WriteResult MetaReplication::write(WalEntryType type, const std::string& payload) {
    uint64_t term = term_.load();
    uint64_t idx  = wal_.append(type, term, payload);

    int total_followers = 0;
    {
        std::lock_guard<std::mutex> lk(mu_);
        total_followers = static_cast<int>(followers_.size());
        ack_counts_[idx] = 0;
    }

    for (auto& [addr, _] : followers_) {
        std::thread([this, addr = addr, idx, type, term, payload]() {
            replicate_to_follower(addr, idx, type, term, payload);
        }).detach();
    }

    int majority_needed = (total_followers + 1) / 2;
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(cfg_.quorum_timeout_ms);

    while (std::chrono::steady_clock::now() < deadline) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (ack_counts_.count(idx) && ack_counts_[idx] >= majority_needed) {
                wal_.commit(idx);
                if (on_commit_) on_commit_(idx);
                spdlog::debug("MetaReplication: committed idx={} term={}", idx, term);
                return WriteResult::OK;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    spdlog::error("MetaReplication: quorum timeout for idx={} — UNAVAILABLE", idx);
    return WriteResult::UNAVAILABLE;
}

void MetaReplication::record_ack(const std::string& follower_addr, uint64_t log_index) {
    std::lock_guard<std::mutex> lk(mu_);
    ack_counts_[log_index]++;
    auto it = followers_.find(follower_addr);
    if (it != followers_.end()) {
        it->second.match_index = std::max(it->second.match_index, log_index);
        it->second.last_ack_ms = now_ms();
        it->second.missed_pings = 0;
        it->second.status = FollowerStatus::HEALTHY;
    }
}

void MetaReplication::send_heartbeats() {
    uint64_t t = now_ms();
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& [addr, fs] : followers_) {
        uint64_t elapsed = t - fs.last_ack_ms;
        if (elapsed > cfg_.heartbeat_interval_ms * 3) {
            fs.missed_pings++;
            if (fs.missed_pings >= 3) {
                fs.status = FollowerStatus::LAGGING;
                spdlog::warn("MetaReplication: follower {} LAGGING (missed {} pings)",
                             addr, fs.missed_pings);
            }
        }
    }
}

void MetaReplication::replicate_to_follower(const std::string& addr,
                                             uint64_t log_index,
                                             WalEntryType type,
                                             uint64_t term,
                                             const std::string& payload) {
    auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    auto stub    = nimbus::repl::MetaReplication::NewStub(channel);

    nimbus::repl::AppendEntryReq req;
    req.set_leader_term(term);
    req.set_prev_log_index(log_index - 1);
    req.set_prev_log_term(term);
    req.set_leader_committed_lsn(wal_.committed_index());

    auto* entry = req.mutable_entry();
    entry->set_log_index(log_index);
    entry->set_term(term);
    entry->set_type(static_cast<nimbus::repl::EntryType>(type));
    entry->set_payload(payload);

    nimbus::repl::AppendEntryResp resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::milliseconds(cfg_.quorum_timeout_ms));

    auto status = stub->AppendEntry(&ctx, req, &resp);
    if (status.ok() && resp.success()) {
        record_ack(addr, log_index);

        nimbus::repl::CommitReq cr;
        cr.set_log_index(log_index);
        cr.set_term(term);
        nimbus::repl::CommitResp cresp;
        grpc::ClientContext ctx2;
        stub->CommitEntry(&ctx2, cr, &cresp);
    } else {
        spdlog::warn("MetaReplication: AppendEntry to {} failed: {}", addr,
                     status.ok() ? resp.error() : status.error_message());

        // Follower rejected due to log gap — replay all missing entries.
        uint64_t follower_match = 0;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = followers_.find(addr);
            if (it != followers_.end()) follower_match = it->second.match_index;
        }
        uint64_t catch_up_from = follower_match + 1;
        spdlog::info("MetaReplication: catch-up {} from idx={} to idx={}",
                     addr, catch_up_from, log_index);

        for (uint64_t idx = catch_up_from; idx <= log_index; ++idx) {
            nimbus::WalEntry e;
            if (!wal_.read_entry(idx, e)) {
                spdlog::warn("MetaReplication: catch-up missing WAL entry idx={}", idx);
                break;
            }

            nimbus::repl::AppendEntryReq cu_req;
            cu_req.set_leader_term(e.term);
            cu_req.set_prev_log_index(idx - 1);
            cu_req.set_prev_log_term(e.term);
            cu_req.set_leader_committed_lsn(wal_.committed_index());
            auto* ce = cu_req.mutable_entry();
            ce->set_log_index(idx);
            ce->set_term(e.term);
            ce->set_type(static_cast<nimbus::repl::EntryType>(e.type));
            ce->set_payload(e.payload);

            nimbus::repl::AppendEntryResp cu_resp;
            grpc::ClientContext cu_ctx;
            cu_ctx.set_deadline(std::chrono::system_clock::now() +
                                std::chrono::milliseconds(cfg_.quorum_timeout_ms));
            auto cu_status = stub->AppendEntry(&cu_ctx, cu_req, &cu_resp);
            if (!cu_status.ok() || !cu_resp.success()) {
                spdlog::warn("MetaReplication: catch-up AppendEntry idx={} to {} failed",
                             idx, addr);
                break;
            }
            record_ack(addr, idx);

            nimbus::repl::CommitReq cr;
            cr.set_log_index(idx);
            cr.set_term(e.term);
            nimbus::repl::CommitResp cresp;
            grpc::ClientContext ctx3;
            stub->CommitEntry(&ctx3, cr, &cresp);
        }
    }
}

} // namespace nimbus
