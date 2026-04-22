#include "../common/config.h"
#include "../common/logger.h"
#include "../storage/chunk_store.h"
#include "../storage/storage_rpc.h"
#include "../storage/stats_reporter.h"
#include <grpcpp/grpcpp.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

static std::atomic<bool> g_running{true};

static void sig_handler(int) { g_running = false; }

static void print_usage() {
    std::cerr
        << "Usage: storage_server\n"
        << "  --id        NODE_ID\n"
        << "  --listen    HOST:PORT\n"
        << "  --data      DATA_DIR\n"
        << "  --meta      META_HOST:PORT\n"
        << "  --capacity  BYTES (default: 10737418240)\n";
}

int main(int argc, char** argv) {
    nimbus::NodeConfig cfg;
    cfg.capacity_bytes = 10737418240ULL;
    std::string meta_addr;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--id" && i + 1 < argc) cfg.node_id = argv[++i];
        else if (arg == "--listen" && i + 1 < argc) cfg.address = argv[++i];
        else if (arg == "--data" && i + 1 < argc) cfg.data_dir = argv[++i];
        else if (arg == "--meta" && i + 1 < argc) meta_addr = argv[++i];
        else if (arg == "--capacity" && i + 1 < argc) {
            cfg.capacity_bytes = static_cast<uint64_t>(std::stoull(argv[++i]));
        } else if (arg == "--help") {
            print_usage();
            return 0;
        }
    }

    if (cfg.node_id.empty() || cfg.address.empty() || cfg.data_dir.empty() || meta_addr.empty()) {
        print_usage();
        return 1;
    }

    std::filesystem::create_directories(cfg.data_dir);
    nimbus::init_logger("storage_" + cfg.node_id, cfg.data_dir + "/storage.log");

    std::signal(SIGINT, sig_handler);
    std::signal(SIGTERM, sig_handler);

    nimbus::ChunkStore store(cfg.data_dir + "/chunks");
    nimbus::StorageRpcService svc(store);
    nimbus::StatsReporter reporter(cfg, meta_addr);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(cfg.address, grpc::InsecureServerCredentials());
    builder.RegisterService(&svc);
    auto server = builder.BuildAndStart();
    if (!server) return 1;

    std::thread hb_thread([&]() {
        while (g_running.load()) {
            reporter.run_once();
            std::this_thread::sleep_for(
                std::chrono::milliseconds(cfg.heartbeat_interval_ms));
        }
    });

    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    reporter.stop();
    server->Shutdown();
    hb_thread.join();
    return 0;
}
