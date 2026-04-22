#include "../meta/meta_wal.h"
#include "../meta/meta_coordinator.h"
#include "../meta/meta_replication.h"
#include "../meta/meta_follower.h"
#include "../meta/meta_snapshot.h"
#include "../meta/meta_rpc.h"
#include "../meta/policy.h"
#include "../meta/placement.h"
#include "../meta/reconfig_driver.h"
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
        // Dispatcher: route each entry type to coordinator.
        // Full parsing wired once proto codegen is done.
        spdlog::debug("WAL replay: type={} idx={}", static_cast<int>(e.type), e.log_index);
    });

    nimbus::MetaReplication* repl    = nullptr;
    nimbus::MetaFollower*    follower = nullptr;

    if (cfg.role == "leader") {
        repl = new nimbus::MetaReplication(wal, cfg, [&](uint64_t idx) {
            spdlog::debug("on_commit: idx={}", idx);
            // Full WAL entry dispatch wired in Day 2 once proto codegen done.
        });
        for (auto& peer : cfg.peers) repl->add_follower(peer);
        spdlog::info("Leader ready with {} followers", cfg.peers.size());
    } else {
        follower = new nimbus::MetaFollower(wal, cfg, [&](uint64_t idx) {
            spdlog::debug("follower on_commit: idx={}", idx);
        });
        spdlog::info("Follower ready, leader at {}", cfg.peers.empty() ? "?" : cfg.peers[0]);
    }

    // ── gRPC servers ──────────────────────────────────────────────────────
    nimbus::MetaRpcService  client_svc(coord, repl, cfg);

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

    // ── Background: heartbeat timer + stale node eviction ─────────────────
    std::thread hb_thread([&]() {
        while (g_running) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(cfg.heartbeat_interval_ms));
            if (repl) repl->send_heartbeats();
            coord.evict_stale_nodes(nimbus::unix_ms(), cfg.follower_timeout_ms * 5);
        }
    });

    spdlog::info("meta_server ready");
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    spdlog::info("Shutting down...");
    server->Shutdown();
    hb_thread.join();

    delete repl;
    delete follower;
    return 0;
}
