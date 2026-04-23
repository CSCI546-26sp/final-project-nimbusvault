#include "meta_snapshot.h"
#include <spdlog/spdlog.h>
#include <fstream>
#include <cstring>
#include <stdexcept>
#include <sstream>

namespace nimbus {

static constexpr uint32_t SNAPSHOT_MAGIC = 0x4E4D5653;  
static void write_lp(std::string& out, const std::string& data) {
    uint32_t len = static_cast<uint32_t>(data.size());
    uint32_t be  = __builtin_bswap32(len);
    out.append(reinterpret_cast<char*>(&be), 4);
    out.append(data);
}

static std::string read_lp(const char* buf, size_t buf_len, size_t& offset) {
    if (offset + 4 > buf_len) throw std::runtime_error("snapshot truncated (len)");
    uint32_t be;
    std::memcpy(&be, buf + offset, 4);
    uint32_t len = __builtin_bswap32(be);
    offset += 4;
    if (offset + len > buf_len) throw std::runtime_error("snapshot truncated (data)");
    std::string data(buf + offset, len);
    offset += len;
    return data;
}

static std::string serialize_chunk(const ChunkEntry& e) {
    std::string s = e.chunk_id + ";" +
                    std::to_string(e.version) + ";" +
                    std::to_string(e.replication_factor) + ";" +
                    std::to_string(e.size_bytes) + ";" +
                    std::to_string(static_cast<int>(e.config_state)) + ";";
    for (auto& n : e.replica_set) s += n + ",";
    s += ";";
    for (auto& n : e.old_replica_set) s += n + ",";
    s += ";";
    return s;
}

static std::string serialize_node(const NodeEntry& n) {
    return n.node_id + ";" + n.address + ";" +
           std::to_string(n.load_fraction) + ";" +
           std::to_string(n.free_bytes) + ";" +
           std::to_string(n.total_bytes) + ";" +
           std::to_string(n.failure_rate_7d) + ";" +
           std::to_string(n.last_seen_ms);
}

static std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : s) {
        if (c == ',') { if (!cur.empty()) { out.push_back(cur); cur.clear(); } }
        else cur.push_back(c);
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

static NodeEntry parse_node(const std::string& s) {
    std::vector<std::string> parts;
    std::string cur;
    for (char c : s) {
        if (c == ';') { parts.push_back(cur); cur.clear(); }
        else cur.push_back(c);
    }
    if (!cur.empty()) parts.push_back(cur);
    if (parts.size() < 7) throw std::runtime_error("malformed node entry");
    NodeEntry n;
    n.node_id         = parts[0];
    n.address         = parts[1];
    n.load_fraction   = std::stof(parts[2]);
    n.free_bytes      = std::stoull(parts[3]);
    n.total_bytes     = std::stoull(parts[4]);
    n.failure_rate_7d = std::stof(parts[5]);
    n.last_seen_ms    = std::stoull(parts[6]);
    n.alive           = true;
    return n;
}

static ChunkEntry parse_chunk(const std::string& s) {
    std::vector<std::string> parts;
    std::string cur;
    for (char c : s) {
        if (c == ';') { parts.push_back(cur); cur.clear(); }
        else cur.push_back(c);
    }
    if (!cur.empty()) parts.push_back(cur);
    if (parts.size() < 7) throw std::runtime_error("malformed chunk entry");
    ChunkEntry e;
    e.chunk_id          = parts[0];
    e.version           = std::stoull(parts[1]);
    e.replication_factor = std::stoi(parts[2]);
    e.size_bytes        = std::stoull(parts[3]);
    e.config_state      = static_cast<ChunkConfigState>(std::stoi(parts[4]));
    e.replica_set       = split_csv(parts[5]);
    e.old_replica_set   = split_csv(parts[6]);
    return e;
}

MetaSnapshot::MetaSnapshot(MetaCoordinator& coord, MetaWal& wal,
                             const std::string& data_dir)
    : coord_(coord), wal_(wal), data_dir_(data_dir) {}

std::pair<std::string, uint64_t> MetaSnapshot::take_snapshot() {
    uint64_t lsn    = wal_.committed_index();
    auto     nodes  = coord_.get_nodes();
    auto     chunks = coord_.get_chunks();

    std::string buf;

    uint32_t magic_be = __builtin_bswap32(SNAPSHOT_MAGIC);
    uint64_t lsn_be   = __builtin_bswap64(lsn);
    buf.append(reinterpret_cast<char*>(&magic_be), 4);
    buf.append(reinterpret_cast<char*>(&lsn_be), 8);

    uint32_t nc_be = __builtin_bswap32(static_cast<uint32_t>(nodes.size()));
    buf.append(reinterpret_cast<char*>(&nc_be), 4);
    for (auto& n : nodes) write_lp(buf, serialize_node(n));

    uint32_t cc_be = __builtin_bswap32(static_cast<uint32_t>(chunks.size()));
    buf.append(reinterpret_cast<char*>(&cc_be), 4);
    for (auto& c : chunks) write_lp(buf, serialize_chunk(c));

    spdlog::info("MetaSnapshot::take_snapshot lsn={} nodes={} chunks={}",
                 lsn, nodes.size(), chunks.size());
    return {buf, lsn};
}

uint64_t MetaSnapshot::install_snapshot(const std::string& snapshot_data) {
    const char* buf = snapshot_data.data();
    size_t      len = snapshot_data.size();
    size_t      off = 0;

    if (len < 16) throw std::runtime_error("snapshot too small");

    uint32_t magic_be;
    std::memcpy(&magic_be, buf + off, 4); off += 4;
    if (__builtin_bswap32(magic_be) != SNAPSHOT_MAGIC)
        throw std::runtime_error("snapshot magic mismatch");

    uint64_t lsn_be;
    std::memcpy(&lsn_be, buf + off, 8); off += 8;
    uint64_t lsn = __builtin_bswap64(lsn_be);

    uint32_t nc_be;
    std::memcpy(&nc_be, buf + off, 4); off += 4;
    uint32_t node_count = __builtin_bswap32(nc_be);

    std::vector<NodeEntry> nodes;
    nodes.reserve(node_count);
    for (uint32_t i = 0; i < node_count; ++i) {
        std::string entry = read_lp(buf, len, off);
        nodes.push_back(parse_node(entry));
    }

    uint32_t cc_be;
    std::memcpy(&cc_be, buf + off, 4); off += 4;
    uint32_t chunk_count = __builtin_bswap32(cc_be);

    std::vector<ChunkEntry> chunks;
    chunks.reserve(chunk_count);
    for (uint32_t i = 0; i < chunk_count; ++i) {
        std::string entry = read_lp(buf, len, off);
        chunks.push_back(parse_chunk(entry));
    }

    coord_.restore_snapshot(nodes, chunks);

    spdlog::info("MetaSnapshot::install_snapshot lsn={} nodes={} chunks={}",
                 lsn, node_count, chunk_count);
    return lsn;
}

} // namespace nimbus
