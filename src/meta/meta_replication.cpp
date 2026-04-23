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
    std::vector<std::string> followers;
    {
        std::lock_guard<std::mutex> lk(mu_);
        total_followers = static_cast<int>(followers_.size());
        ack_counts_[idx] = 0;
        followers.reserve(followers_.size());
        for (const auto& [addr, _] : followers_) followers.push_back(addr);
    }

    for (const auto& addr : followers) {
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
    auto ac = ack_counts_.find(log_index);
    if (ac != ack_counts_.end()) {
        ac->second++;
    }
    auto it = followers_.find(follower_addr);
    if (it != followers_.end()) {
        bool was_lagging = (it->second.status == FollowerStatus::LAGGING);
        it->second.match_index = std::max(it->second.match_index, log_index);
        it->second.next_index = std::max(it->second.next_index, log_index + 1);
        it->second.last_ack_ms = now_ms();
        it->second.missed_pings = 0;
        it->second.status = FollowerStatus::HEALTHY;
        if (was_lagging) {
            spdlog::info("MetaReplication: follower {} recovered at idx={}",
                         follower_addr, it->second.match_index);
        }
    }
}

void MetaReplication::send_heartbeats() {
    uint64_t t = now_ms();
    uint64_t leader_last = wal_.last_log_index();
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& [addr, fs] : followers_) {
        if (fs.match_index >= leader_last) {
            fs.missed_pings = 0;
            fs.status = FollowerStatus::HEALTHY;
            continue;
        }

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
    (void)type;
    (void)payload;

    auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    auto stub    = nimbus::repl::MetaReplication::NewStub(channel);

    uint64_t next_idx = 1;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = followers_.find(addr);
        if (it == followers_.end()) return;
        next_idx = std::min(it->second.next_index, log_index);
    }

    if (next_idx < log_index) {
        spdlog::info("MetaReplication: catch-up {} from idx={} to idx={}",
                     addr, next_idx, log_index);
    }

    while (next_idx <= log_index) {
        WalEntry e;
        if (!wal_.read_entry(next_idx, e)) {
            spdlog::warn("MetaReplication: missing WAL entry idx={} for follower {}",
                         next_idx, addr);
            return;
        }

        nimbus::repl::AppendEntryReq req;
        req.set_leader_term(term);
        req.set_prev_log_index(next_idx > 0 ? next_idx - 1 : 0);
        req.set_prev_log_term(term);
        req.set_leader_committed_lsn(wal_.committed_index());

        auto* entry = req.mutable_entry();
        entry->set_log_index(e.log_index);
        entry->set_term(e.term);
        entry->set_type(static_cast<nimbus::repl::EntryType>(e.type));
        entry->set_payload(e.payload);

        nimbus::repl::AppendEntryResp resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(cfg_.quorum_timeout_ms));

        auto status = stub->AppendEntry(&ctx, req, &resp);
        if (status.ok() && resp.success()) {
            record_ack(addr, next_idx);

            nimbus::repl::CommitReq cr;
            cr.set_log_index(next_idx);
            cr.set_term(term);
            nimbus::repl::CommitResp cresp;
            grpc::ClientContext ctx2;
            stub->CommitEntry(&ctx2, cr, &cresp);

            next_idx++;
            continue;
        }

        if (!status.ok()) {
            spdlog::warn("MetaReplication: AppendEntry to {} failed: {}", addr,
                         status.error_message());
            return;
        }

        if (next_idx <= 1) {
            spdlog::warn("MetaReplication: follower {} rejected idx=1 append ({})",
                         addr, resp.error());
            return;
        }

        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = followers_.find(addr);
            if (it != followers_.end()) {
                if (it->second.next_index > 1) it->second.next_index--;
                next_idx = it->second.next_index;
            } else {
                next_idx--;
            }
        }
    }
}

} // namespace nimbus
