#include "../meta/meta_wal.h"
#include "../meta/meta_coordinator.h"
#include "../meta/meta_replication.h"
#include "../meta/meta_follower.h"
#include "../meta/meta_snapshot.h"
#include "../meta/meta_rpc.h"
#include "../meta/policy.h"
#include "../meta/placement.h"
#include "../meta/reconfig_driver.h"
#include "../meta/recovery.h"
#include "../common/config.h"
#include "../common/logger.h"
#include "../common/clock.h"
#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>
#include <vector>
#include <memory>
#include "storage.grpc.pb.h"

static std::atomic<bool> g_running{true};

static void sig_handler(int) { g_running = false; }

static void print_usage() {
    std::cerr <<
        "Usage: meta_server\n"
        "  --role   leader|follower\n"
        "  --id     NODE_ID\n"
        "  --listen HOST:PORT\n"
        "  --data   DATA_DIR\n"
        "  --peers  HOST:PORT,...   (followers if leader; leader if follower)\n"
        "  --mode   baseline|adaptive  (default: adaptive)\n";
}

static std::vector<std::string> split_nonempty(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : s) {
        if (c == delim) {
            if (!cur.empty()) out.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

static void delete_chunk_from_replica(const std::string& addr,
                                      const std::string& chunk_id) {
    if (addr.empty()) return;
    auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    auto stub = nimbus::storage::StorageNode::NewStub(channel);

    nimbus::storage::DeleteDataReq req;
    req.set_chunk_id(chunk_id);
    nimbus::storage::DeleteDataResp resp;
    grpc::ClientContext ctx;
    (void)stub->DeleteChunkData(&ctx, req, &resp);
}

static bool bootstrap_follower_from_leader(nimbus::MetaFollower& follower,
                                           nimbus::MetaSnapshot& snapshot,
                                           const nimbus::NodeConfig& cfg) {
    if (cfg.peers.empty()) return false;

    const std::string& leader_addr = cfg.peers.front();
    auto channel = grpc::CreateChannel(leader_addr, grpc::InsecureChannelCredentials());
    auto stub = nimbus::repl::MetaReplication::NewStub(channel);

    grpc::ClientContext snapshot_ctx;
    nimbus::repl::SnapshotReq req;
    req.set_snapshot_lsn(0);
    std::unique_ptr<grpc::ClientReader<nimbus::repl::SnapshotChunk>> reader(
        stub->InstallSnapshot(&snapshot_ctx, req));

    std::string snapshot_data;
    uint64_t snapshot_lsn = 0;
    bool received = false;
    nimbus::repl::SnapshotChunk chunk;
    while (reader->Read(&chunk)) {
        snapshot_data.append(chunk.data());
        snapshot_lsn = chunk.snapshot_lsn();
        received = true;
        if (chunk.is_last_chunk()) break;
    }

    auto snapshot_status = reader->Finish();
    if (!snapshot_status.ok() || !received) {
        spdlog::warn("bootstrap_follower_from_leader: snapshot fetch failed from {}: {}",
                     leader_addr, snapshot_status.error_message());
        return false;
    }

    try {
        snapshot_lsn = snapshot.install_snapshot(snapshot_data);
        follower.install_snapshot(snapshot_data, snapshot_lsn);
    } catch (const std::exception& ex) {
        spdlog::warn("bootstrap_follower_from_leader: snapshot install failed: {}", ex.what());
        return false;
    }

    grpc::ClientContext log_ctx;
    nimbus::repl::FetchLogReq log_req;
    log_req.set_from_index(snapshot_lsn + 1);
    std::unique_ptr<grpc::ClientReader<nimbus::repl::LogEntry>> log_reader(
        stub->FetchLogEntries(&log_ctx, log_req));

    uint64_t prev_log_index = snapshot_lsn;
    uint64_t prev_log_term  = 0;
    nimbus::repl::LogEntry entry;
    while (log_reader->Read(&entry)) {
        const auto type = static_cast<nimbus::WalEntryType>(entry.type());
        const bool ok = follower.handle_append(
            entry.term(),
            prev_log_index,
            prev_log_term,
            entry.log_index(),
            entry.term(),
            type,
            entry.payload(),
            snapshot_lsn);
        if (!ok) {
            spdlog::warn("bootstrap_follower_from_leader: append failed at idx={}",
                         entry.log_index());
            break;
        }
        follower.handle_commit(entry.log_index(), entry.term());
        prev_log_index = entry.log_index();
        prev_log_term  = entry.term();
    }

    auto log_status = log_reader->Finish();
    if (!log_status.ok()) {
        spdlog::warn("bootstrap_follower_from_leader: log fetch failed from {}: {}",
                     leader_addr, log_status.error_message());
        return false;
    }

    spdlog::info("bootstrap_follower_from_leader: installed snapshot lsn={} and replayed logs from {}",
                 snapshot_lsn, snapshot_lsn + 1);
    return true;
}

static bool parse_heartbeat_payload(const std::string& payload,
                                    std::string& node_id,
                                    std::string& address,
                                    float& load,
                                    uint64_t& free_bytes,
                                    uint64_t& total_bytes,
                                    float& fail_rate) {
    auto parts = split_nonempty(payload, '|');
    if (parts.size() < 6) return false;

    node_id = parts[0];
    address = parts[1];
    try {
        load = std::stof(parts[2]);
        free_bytes = static_cast<uint64_t>(std::stoull(parts[3]));
        total_bytes = static_cast<uint64_t>(std::stoull(parts[4]));
        fail_rate = std::stof(parts[5]);
    } catch (...) {
        return false;
    }
    return true;
}

static void apply_wal_entry(nimbus::MetaCoordinator& coord, const nimbus::WalEntry& e) {
    switch (e.type) {
        case nimbus::WalEntryType::PUT_CHUNK: {
            auto parts = split_nonempty(e.payload, '|');
            if (parts.size() < 5) {
                spdlog::warn("apply_wal_entry: malformed PUT_CHUNK payload idx={}", e.log_index);
                return;
            }
            std::vector<std::string> replicas = split_nonempty(parts[4], ',');
            coord.apply_put_chunk(parts[0],
                                  static_cast<uint64_t>(std::stoull(parts[1])),
                                  std::stoi(parts[2]),
                                  replicas,
                                  static_cast<uint64_t>(std::stoull(parts[3])));
            return;
        }
        case nimbus::WalEntryType::DELETE_CHUNK: {
            coord.apply_delete_chunk(e.payload);
            return;
        }
        case nimbus::WalEntryType::RECONFIGURE_START: {
            auto parts = split_nonempty(e.payload, '|');
            if (parts.size() < 3) {
                spdlog::warn("apply_wal_entry: malformed RECONFIGURE_START payload idx={}", e.log_index);
                return;
            }
            auto old_set = split_nonempty(parts[1], ',');
            auto new_set = split_nonempty(parts[2], ',');
            coord.apply_reconfig_start(parts[0], old_set, new_set);
            return;
        }
        case nimbus::WalEntryType::RECONFIGURE_COMMIT: {
            auto parts = split_nonempty(e.payload, '|');
            if (parts.size() < 2) {
                spdlog::warn("apply_wal_entry: malformed RECONFIGURE_COMMIT payload idx={}", e.log_index);
                return;
            }
            auto new_set = split_nonempty(parts[1], ',');
            coord.apply_reconfig_commit(parts[0], new_set);
            return;
        }
        case nimbus::WalEntryType::NODE_HEARTBEAT: {
            std::string node_id;
            std::string address;
            float load = 0.0f;
            uint64_t free_bytes = 0;
            uint64_t total_bytes = 0;
            float fail_rate = 0.0f;
            if (!parse_heartbeat_payload(e.payload,
                                         node_id,
                                         address,
                                         load,
                                         free_bytes,
                                         total_bytes,
                                         fail_rate)) {
                spdlog::warn("apply_wal_entry: malformed NODE_HEARTBEAT payload idx={}", e.log_index);
                return;
            }
            coord.apply_heartbeat(node_id, address, load, free_bytes, total_bytes, fail_rate);
            return;
        }
        default:
            return;
    }
}

int main(int argc, char** argv) {
    nimbus::NodeConfig cfg;
    cfg.mode = "adaptive";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--role"   && i+1 < argc) cfg.role    = argv[++i];
        else if (arg == "--id"   && i+1 < argc) cfg.node_id  = argv[++i];
        else if (arg == "--listen" && i+1 < argc) cfg.address  = argv[++i];
        else if (arg == "--data" && i+1 < argc) cfg.data_dir = argv[++i];
        else if (arg == "--mode" && i+1 < argc) cfg.mode     = argv[++i];
        else if (arg == "--peers" && i+1 < argc) {
            std::string peers_str = argv[++i];
            std::string p;
            for (char c : peers_str) {
                if (c == ',') { if (!p.empty()) { cfg.peers.push_back(p); p.clear(); } }
                else p += c;
            }
            if (!p.empty()) cfg.peers.push_back(p);
        }
        else if (arg == "--help") { print_usage(); return 0; }
    }

    if (cfg.role.empty() || cfg.address.empty() || cfg.data_dir.empty()) {
        print_usage();
        return 1;
    }

    nimbus::init_logger("meta_" + cfg.node_id, cfg.data_dir + "/meta.log");
    spdlog::info("meta_server starting: role={} id={} listen={} mode={}",
                 cfg.role, cfg.node_id, cfg.address, cfg.mode);

    std::signal(SIGINT,  sig_handler);
    std::signal(SIGTERM, sig_handler);

    // ── Core objects ──────────────────────────────────────────────────────
    nimbus::MetaWal         wal(cfg.data_dir);
    nimbus::MetaCoordinator coord(cfg);
    nimbus::MetaSnapshot    snapshot(coord, wal, cfg.data_dir);

    // Replay WAL on startup.
    wal.replay([&](const nimbus::WalEntry& e) {
        apply_wal_entry(coord, e);
    });

    if (cfg.role == "leader") {
        nimbus::cleanup_uncommitted_puts(wal, coord, delete_chunk_from_replica);
    }

    nimbus::MetaReplication* repl    = nullptr;
    nimbus::MetaFollower*    follower = nullptr;

    if (cfg.role == "leader") {
        repl = new nimbus::MetaReplication(wal, cfg, [&](uint64_t idx) {
            nimbus::WalEntry e;
            if (!wal.read_entry(idx, e)) {
                spdlog::warn("on_commit: missing WAL entry idx={}", idx);
                return;
            }
            apply_wal_entry(coord, e);
        });
        for (auto& peer : cfg.peers) repl->add_follower(peer);
        spdlog::info("Leader ready with {} followers", cfg.peers.size());
    } else {
        follower = new nimbus::MetaFollower(wal, cfg, [&](uint64_t idx) {
            nimbus::WalEntry e;
            if (!wal.read_entry(idx, e)) {
                spdlog::warn("follower on_commit: missing WAL entry idx={}", idx);
                return;
            }
            apply_wal_entry(coord, e);
        });
        spdlog::info("Follower ready, leader at {}", cfg.peers.empty() ? "?" : cfg.peers[0]);
    }

    // ── Policy engine + reconfig (leader + adaptive mode only) ───────────
    nimbus::AdaptivePolicy*  policy_ptr   = nullptr;
    nimbus::ReconfigDriver*  reconfig_ptr = nullptr;
    nimbus::Placement*       placement_ptr = nullptr;

    if (cfg.role == "leader" && cfg.mode == "adaptive") {
        nimbus::PlacementConfig pcfg;  // multi-dim scoring (random_placement = false)
        placement_ptr = new nimbus::Placement(pcfg);
        nimbus::PolicyConfig policy_cfg;  // defaults: cold_rf=2 warm_rf=3 hot_rf=5
        policy_ptr    = new nimbus::AdaptivePolicy(policy_cfg);
        reconfig_ptr  = new nimbus::ReconfigDriver(coord, *repl, *placement_ptr);
        spdlog::info("Adaptive policy engine enabled");
    }

    // ── gRPC servers ──────────────────────────────────────────────────────
    nimbus::MetaRpcService  client_svc(coord, repl, cfg, policy_ptr, reconfig_ptr);

    // Reuse a dummy follower on leader for the repl service (leader also handles
    // FetchLogEntries for catch-up). On leader, follower ptr is null — we create
    // a thin one just to satisfy MetaReplService constructor.
    nimbus::MetaFollower dummy_follower(wal, cfg, nullptr);
    nimbus::MetaReplService repl_svc(
        follower ? *follower : dummy_follower,
        snapshot,
        repl);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(cfg.address, grpc::InsecureServerCredentials());
    builder.RegisterService(&client_svc);
    builder.RegisterService(&repl_svc);
    auto server = builder.BuildAndStart();
    spdlog::info("gRPC server listening on {}", cfg.address);

    if (follower) {
        (void)bootstrap_follower_from_leader(*follower, snapshot, cfg);
    }

    // ── Background: heartbeat timer + stale node eviction + policy eval ──
    std::thread hb_thread([&]() {
        uint32_t policy_tick = 0;
        const uint32_t policy_every = 5;  // evaluate policy every 5 heartbeat intervals
        while (g_running) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(cfg.heartbeat_interval_ms));
            if (repl) repl->send_heartbeats();
            coord.evict_stale_nodes(nimbus::unix_ms(), cfg.follower_timeout_ms * 5);

            if (policy_ptr && reconfig_ptr && ++policy_tick >= policy_every) {
                policy_tick = 0;
                auto chunks = coord.get_chunks();
                for (const auto& c : chunks) {
                    auto dec = policy_ptr->evaluate(c.chunk_id);
                    if (dec.changed && dec.new_rf != c.replication_factor) {
                        spdlog::info("policy: chunk={} tier-change rf {} → {}",
                                     c.chunk_id, c.replication_factor, dec.new_rf);
                        reconfig_ptr->reconfig(c.chunk_id, dec.new_rf);
                    }
                }
            }
        }
    });

    spdlog::info("meta_server ready");
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    spdlog::info("Shutting down...");
    server->Shutdown();
    hb_thread.join();

    delete reconfig_ptr;
    delete policy_ptr;
    delete placement_ptr;
    delete repl;
    delete follower;
    return 0;
}
