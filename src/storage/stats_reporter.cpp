#include "stats_reporter.h"
#include <grpcpp/create_channel.h>
#include <spdlog/spdlog.h>
#include <thread>

#if defined(__linux__) || defined(__APPLE__)
#include <sys/statvfs.h>
#include <unistd.h>
#endif

namespace nimbus {

StatsReporter::StatsReporter(const NodeConfig& cfg, const std::string& meta_addr)
    : cfg_(cfg) {
    // meta_addr may be comma-separated list of all meta nodes for failover.
    std::string cur;
    for (char c : meta_addr) {
        if (c == ',') { if (!cur.empty()) { meta_addrs_.push_back(cur); cur.clear(); } }
        else cur += c;
    }
    if (!cur.empty()) meta_addrs_.push_back(cur);
    if (meta_addrs_.empty()) meta_addrs_.push_back(meta_addr);
    reconnect(meta_addrs_[0]);
}

void StatsReporter::reconnect(const std::string& addr) {
    channel_ = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    stub_ = nimbus::meta::MetaCoordinator::NewStub(channel_);
    spdlog::debug("StatsReporter: connected to meta {}", addr);
}

void StatsReporter::record_access(const std::string& chunk_id) {
    std::lock_guard<std::mutex> lk(mu_);
    access_counts_[chunk_id]++;
}

void StatsReporter::run_once() {
    if (stopped_.load()) return;

    nimbus::meta::HeartbeatReq req;
    req.set_node_id(cfg_.node_id);
    req.set_address(cfg_.address);
    req.set_load_fraction(compute_load_fraction());

    uint64_t free_bytes = 0;
    uint64_t total_bytes = 0;
    if (compute_disk_usage(free_bytes, total_bytes)) {
        req.set_free_bytes(free_bytes);
        req.set_total_bytes(total_bytes);
    } else {
        req.set_free_bytes(0);
        req.set_total_bytes(0);
    }
    req.set_failure_rate_7d(0.0f);

    {
        std::lock_guard<std::mutex> lk(mu_);
        auto* m = req.mutable_chunk_access_counts();
        for (const auto& [k, v] : access_counts_) {
            (*m)[k] = v;
        }
        access_counts_.clear();
    }

    nimbus::meta::HeartbeatResp resp;
    grpc::ClientContext ctx;
    grpc::Status status = stub_->NodeHeartbeat(&ctx, req, &resp);

    if (!status.ok()) {
        // Connection failed — try next meta address in the list.
        if (meta_addrs_.size() > 1) {
            current_meta_idx_ = (current_meta_idx_ + 1) % meta_addrs_.size();
            spdlog::info("StatsReporter: meta unreachable, trying {}",
                         meta_addrs_[current_meta_idx_]);
            reconnect(meta_addrs_[current_meta_idx_]);
        }
        return;
    }

    if (!resp.ok() && !resp.leader_hint().empty()) {
        // Follower redirected us to the actual leader.
        const std::string& hint = resp.leader_hint();
        spdlog::info("StatsReporter: redirected to leader {}", hint);
        reconnect(hint);
        // Also update our primary so we reconnect here on next cycle.
        for (size_t i = 0; i < meta_addrs_.size(); ++i) {
            if (meta_addrs_[i] == hint) { current_meta_idx_ = i; break; }
        }
        return;
    }

    if (status.ok() && resp.ok()) {
        std::vector<std::string> assigned;
        assigned.assign(resp.assigned_chunk_ids().begin(), resp.assigned_chunk_ids().end());

        std::lock_guard<std::mutex> lk(mu_);
        assigned_chunk_ids_ = std::move(assigned);
        has_assignment_snapshot_ = true;
        spdlog::debug("assigned chunk snapshot: node={} chunks={}",
                      cfg_.node_id, assigned_chunk_ids_.size());
    }
}

void StatsReporter::stop() {
    stopped_.store(true);
}

std::vector<std::string> StatsReporter::assigned_chunk_ids() const {
    std::lock_guard<std::mutex> lk(mu_);
    return assigned_chunk_ids_;
}

bool StatsReporter::has_assignment_snapshot() const {
    std::lock_guard<std::mutex> lk(mu_);
    return has_assignment_snapshot_;
}

float StatsReporter::compute_load_fraction() const {
#if defined(__linux__) || defined(__APPLE__)
    double load = 0.0;
    if (getloadavg(&load, 1) != 1) {
        return 0.0f;
    }

    long cpus = std::thread::hardware_concurrency();
    if (cpus <= 0) cpus = 1;

    double frac = load / static_cast<double>(cpus);
    if (frac < 0.0) frac = 0.0;
    if (frac > 1.0) frac = 1.0;
    return static_cast<float>(frac);
#else
    return 0.0f;
#endif
}

bool StatsReporter::compute_disk_usage(uint64_t& free_bytes, uint64_t& total_bytes) const {
#if defined(__linux__) || defined(__APPLE__)
    struct statvfs st {};
    if (statvfs(cfg_.data_dir.c_str(), &st) != 0) {
        return false;
    }

    free_bytes = static_cast<uint64_t>(st.f_bavail) * static_cast<uint64_t>(st.f_bsize);
    total_bytes = static_cast<uint64_t>(st.f_blocks) * static_cast<uint64_t>(st.f_frsize);
    return true;
#else
    free_bytes = 0;
    total_bytes = 0;
    return false;
#endif
}

} // namespace nimbus
