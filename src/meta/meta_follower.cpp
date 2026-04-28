#include "meta_follower.h"
#include <spdlog/spdlog.h>

namespace nimbus {

MetaFollower::MetaFollower(MetaWal& wal, const NodeConfig& cfg, CommitCallback on_commit)
    : wal_(wal), cfg_(cfg), on_commit_(std::move(on_commit)) {}

bool MetaFollower::handle_append(uint64_t leader_term,
                                  uint64_t prev_log_index, uint64_t ,
                                  uint64_t log_index, uint64_t term,
                                  WalEntryType type, const std::string& payload,
                                  uint64_t ) {
    if (leader_term < current_term_) {
        spdlog::warn("MetaFollower: rejecting AppendEntry — stale leader term {} < {}",
                     leader_term, current_term_);
        return false;
    }
    current_term_ = leader_term;

    const uint64_t local_last = wal_.last_log_index();

    if (log_index <= local_last) {
        WalEntry existing;
        if (!wal_.read_entry(log_index, existing)) {
            // Under concurrent AppendEntry handlers, local_last can advance before
            // the corresponding entry is visible to this thread; treat as idempotent.
            spdlog::debug("MetaFollower: duplicate idx={} observed during append race", log_index);
            return true;
        }
        if (existing.term != term || existing.type != type || existing.payload != payload) {
            spdlog::warn("MetaFollower: conflicting entry at idx={} (local term/type/payload mismatch)",
                         log_index);
            return false;
        }
        spdlog::debug("MetaFollower: duplicate/old AppendEntry idx={} ignored (local_last={})",
                      log_index, local_last);
        return true;
    }

    if (log_index > local_last + 1) {
        spdlog::warn("MetaFollower: log gap detected: prev={} local_last={}",
                     prev_log_index, local_last);
        return false;
    }

    if (prev_log_index != local_last) {
        spdlog::warn("MetaFollower: prev index mismatch: prev={} local_last={} idx={}",
                     prev_log_index, local_last, log_index);
        return false;
    }

    wal_.append(type, term, payload);
    spdlog::debug("MetaFollower: appended idx={}", log_index);
    return true;
}

void MetaFollower::handle_commit(uint64_t log_index, uint64_t ) {
    if (log_index > wal_.last_log_index()) {
        spdlog::warn("MetaFollower: commit idx={} > last_log={} — cannot apply yet",
                     log_index, wal_.last_log_index());
        return;
    }
    wal_.commit(log_index);
    if (on_commit_) on_commit_(log_index);
    spdlog::debug("MetaFollower: committed idx={}", log_index);
}

void MetaFollower::install_snapshot(const std::string& ,
                                     uint64_t snapshot_lsn) {
    if (snapshot_lsn < snapshot_lsn_) {
        spdlog::debug("MetaFollower: ignoring stale snapshot lsn={} current={}",
                      snapshot_lsn, snapshot_lsn_);
        return;
    }

    snapshot_lsn_ = snapshot_lsn;
    spdlog::info("MetaFollower: installed snapshot lsn={}", snapshot_lsn);
}

uint64_t MetaFollower::catch_up_from() const {
    return wal_.committed_index() + 1;
}

uint64_t MetaFollower::local_committed() const {
    return wal_.committed_index();
}

uint64_t MetaFollower::local_last_log_index() const {
    return wal_.last_log_index();
}

bool MetaFollower::read_entry(uint64_t log_index, WalEntry& out) const {
    return wal_.read_entry(log_index, out);
}

} // namespace nimbus
