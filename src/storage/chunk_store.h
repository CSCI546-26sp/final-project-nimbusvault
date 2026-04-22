#pragma once
#include <string>
#include <cstdint>
#include <vector>

namespace nimbus {

class ChunkStore {
public:
    explicit ChunkStore(const std::string& data_dir);
    ~ChunkStore();

    // Atomic put. key = chunk_id + "_" + version.
    bool write(const std::string& chunk_id, uint64_t version, const std::string& data);

    // Read latest version >= min_version.
    bool read(const std::string& chunk_id, uint64_t min_version,
              std::string& data_out, uint64_t& version_out);

    // Range delete all versions of chunk.
    bool remove(const std::string& chunk_id);

    // Latest stored version for a chunk (0 = not found).
    uint64_t latest_version(const std::string& chunk_id);

    // List chunk ids currently present in local storage.
    std::vector<std::string> list_chunk_ids();

private:
    struct Impl;
    Impl* impl_;
};

} // namespace nimbus
