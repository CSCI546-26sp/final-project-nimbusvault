// nimbus_cli: command-line interface for NimbusVault
//
// Usage:
//   nimbus_cli [--meta ADDR] [--nodes node_id=addr,...] <command> [args...]
//
// Commands:
//   put <chunk_id> <data>              Write data string to a chunk
//   get <chunk_id>                     Read chunk data and print to stdout
//   del <chunk_id>                     Delete a chunk
//   info <chunk_id>                    Show chunk placement and replication state
//   add-replica <chunk_id> <node_id>   Manually add a replica for a chunk
//   remove-replica <chunk_id> <node_id> Manually remove a replica from a chunk
//
// Example:
//   nimbus_cli --meta 127.0.0.1:9100 put my-chunk "hello world"
//   nimbus_cli --meta 127.0.0.1:9100 get my-chunk
//   nimbus_cli --meta 127.0.0.1:9100 info my-chunk
//   nimbus_cli --meta 127.0.0.1:9100 add-replica my-chunk sn3

#include <iostream>
#include <string>
#include <map>
#include <sstream>
#include <grpcpp/grpcpp.h>
#include "meta.grpc.pb.h"
#include "client/nimbus_client.h"

static void usage() {
    std::cerr <<
        "Usage: nimbus_cli [--meta ADDR] [--nodes ID=ADDR,...] <command> [args]\n"
        "\n"
        "Commands:\n"
        "  put <chunk_id> <data>                Write data to chunk\n"
        "  get <chunk_id>                        Read chunk data\n"
        "  del <chunk_id>                        Delete chunk\n"
        "  info <chunk_id>                       Show chunk placement info\n"
        "  add-replica <chunk_id> <node_id>      Add replica for chunk\n"
        "  remove-replica <chunk_id> <node_id>   Remove replica from chunk\n";
}

static std::map<std::string, std::string> parse_nodes(const std::string& s) {
    std::map<std::string, std::string> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        auto pos = item.find('=');
        if (pos == std::string::npos) continue;
        out[item.substr(0, pos)] = item.substr(pos + 1);
    }
    return out;
}

static const char* config_state_str(nimbus::meta::ConfigState s) {
    switch (s) {
        case nimbus::meta::STABLE:       return "STABLE";
        case nimbus::meta::TRANSITIONING: return "TRANSITIONING";
        default:                         return "UNKNOWN";
    }
}

int cmd_info(nimbus::meta::MetaCoordinator::Stub& stub, const std::string& chunk_id) {
    nimbus::meta::ChunkInfoReq req;
    req.set_chunk_id(chunk_id);
    nimbus::meta::ChunkInfoResp resp;
    grpc::ClientContext ctx;
    auto st = stub.GetChunkInfo(&ctx, req, &resp);
    if (!st.ok()) {
        std::cerr << "RPC failed: " << st.error_message() << "\n";
        return 1;
    }
    if (!resp.ok()) {
        std::cerr << "Error: " << resp.error() << "\n";
        return 1;
    }
    const auto& m = resp.meta();
    std::cout << "chunk_id:    " << m.chunk_id() << "\n";
    std::cout << "version:     " << m.version() << "\n";
    std::cout << "size_bytes:  " << m.size_bytes() << "\n";
    std::cout << "rf:          " << m.replication_factor() << "\n";
    std::cout << "state:       " << config_state_str(m.config_state()) << "\n";
    std::cout << "replicas:\n";
    for (const auto& r : m.replica_set())
        std::cout << "  " << r << "\n";
    if (m.old_replica_set_size() > 0) {
        std::cout << "old_replicas (transitioning):\n";
        for (const auto& r : m.old_replica_set())
            std::cout << "  " << r << "\n";
    }
    return 0;
}

int cmd_add_replica(nimbus::meta::MetaCoordinator::Stub& stub,
                    const std::string& chunk_id, const std::string& node_id) {
    nimbus::meta::ReplicaChangeReq req;
    req.set_chunk_id(chunk_id);
    req.set_node_id(node_id);
    nimbus::meta::ReplicaChangeResp resp;
    grpc::ClientContext ctx;
    auto st = stub.AddReplica(&ctx, req, &resp);
    if (!st.ok()) {
        std::cerr << "RPC failed: " << st.error_message() << "\n";
        return 1;
    }
    if (!resp.ok()) {
        std::cerr << "Error: " << resp.error() << "\n";
        return 1;
    }
    std::cout << "Replica added: " << node_id << " -> " << chunk_id << "\n";
    return 0;
}

int cmd_remove_replica(nimbus::meta::MetaCoordinator::Stub& stub,
                       const std::string& chunk_id, const std::string& node_id) {
    nimbus::meta::ReplicaChangeReq req;
    req.set_chunk_id(chunk_id);
    req.set_node_id(node_id);
    nimbus::meta::ReplicaChangeResp resp;
    grpc::ClientContext ctx;
    auto st = stub.RemoveReplica(&ctx, req, &resp);
    if (!st.ok()) {
        std::cerr << "RPC failed: " << st.error_message() << "\n";
        return 1;
    }
    if (!resp.ok()) {
        std::cerr << "Error: " << resp.error() << "\n";
        return 1;
    }
    std::cout << "Replica removed: " << node_id << " from " << chunk_id << "\n";
    return 0;
}

int main(int argc, char** argv) {
    std::string meta_addr = "127.0.0.1:9100";
    std::map<std::string, std::string> node_addrs;
    int argi = 1;

    // Parse global flags before the command.
    while (argi < argc) {
        std::string a(argv[argi]);
        if (a == "--meta" && argi + 1 < argc) {
            meta_addr = argv[++argi];
        } else if (a == "--nodes" && argi + 1 < argc) {
            node_addrs = parse_nodes(argv[++argi]);
        } else if (a.rfind("--meta=", 0) == 0) {
            meta_addr = a.substr(7);
        } else if (a.rfind("--nodes=", 0) == 0) {
            node_addrs = parse_nodes(a.substr(8));
        } else {
            break; // reached the command
        }
        ++argi;
    }

    if (argi >= argc) {
        usage();
        return 1;
    }

    std::string cmd(argv[argi++]);

    // For put/get/del we go through NimbusClient (handles stub caching + retries).
    // For info/add-replica/remove-replica we talk to meta directly.

    if (cmd == "put") {
        if (argi + 1 >= argc) {
            std::cerr << "Usage: nimbus_cli put <chunk_id> <data>\n";
            return 1;
        }
        std::string chunk_id(argv[argi]);
        std::string data(argv[argi + 1]);
        nimbus::NimbusClient client(meta_addr, node_addrs);
        auto res = client.put(chunk_id, data);
        if (!res.ok) {
            std::cerr << "PUT failed: " << res.error << "\n";
            return 1;
        }
        std::cout << "OK version=" << res.version
                  << " replicas=" << res.replica_nodes.size() << "\n";
        return 0;
    }

    if (cmd == "get") {
        if (argi >= argc) {
            std::cerr << "Usage: nimbus_cli get <chunk_id>\n";
            return 1;
        }
        std::string chunk_id(argv[argi]);
        nimbus::NimbusClient client(meta_addr, node_addrs);
        auto res = client.get(chunk_id);
        if (!res.ok) {
            std::cerr << "GET failed: " << res.error << "\n";
            return 1;
        }
        std::cout << res.data << "\n";
        return 0;
    }

    if (cmd == "del") {
        if (argi >= argc) {
            std::cerr << "Usage: nimbus_cli del <chunk_id>\n";
            return 1;
        }
        std::string chunk_id(argv[argi]);
        nimbus::NimbusClient client(meta_addr, node_addrs);
        bool ok = client.remove(chunk_id);
        if (!ok) {
            std::cerr << "DEL failed\n";
            return 1;
        }
        std::cout << "Deleted " << chunk_id << "\n";
        return 0;
    }

    // Admin commands — talk directly to meta RPC.
    auto meta_channel = grpc::CreateChannel(meta_addr, grpc::InsecureChannelCredentials());
    auto meta_stub    = nimbus::meta::MetaCoordinator::NewStub(meta_channel);

    if (cmd == "info") {
        if (argi >= argc) {
            std::cerr << "Usage: nimbus_cli info <chunk_id>\n";
            return 1;
        }
        return cmd_info(*meta_stub, argv[argi]);
    }

    if (cmd == "add-replica") {
        if (argi + 1 >= argc) {
            std::cerr << "Usage: nimbus_cli add-replica <chunk_id> <node_id>\n";
            return 1;
        }
        return cmd_add_replica(*meta_stub, argv[argi], argv[argi + 1]);
    }

    if (cmd == "remove-replica") {
        if (argi + 1 >= argc) {
            std::cerr << "Usage: nimbus_cli remove-replica <chunk_id> <node_id>\n";
            return 1;
        }
        return cmd_remove_replica(*meta_stub, argv[argi], argv[argi + 1]);
    }

    std::cerr << "Unknown command: " << cmd << "\n\n";
    usage();
    return 1;
}
