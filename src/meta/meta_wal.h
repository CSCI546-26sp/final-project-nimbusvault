#pragma once
#include <string>
#include <functional>
#include <cstdint>

namespace nimbus {

// WAL entry types mirror proto EntryType enum.
enum class WalEntryType : uint8_t {
    PUT_CHUNK          = 0,
    DELETE_CHUNK       = 1,
    RECONFIGURE_START  = 2,
    RECONFIGURE_COMMIT = 3,
    NODE_HEARTBEAT     = 4,
    POLICY_UPDATE      = 5,
};

struct WalEntry {
    uint64_t     log_index = 0;
    uint64_t     term      = 0;
    WalEntryType type;
    std::string  payload;  
};

using ReplayCallback = std::function<void(const WalEntry&)>;

class MetaWal {
public:
    explicit MetaWal(const std::string& data_dir);
    ~MetaWal();

    uint64_t append(WalEntryType type, uint64_t term, const std::string& payload);

    void commit(uint64_t log_index);

    void replay(const ReplayCallback& cb);

    bool read_entry(uint64_t log_index, WalEntry& out) const;

    uint64_t last_log_index() const;
    uint64_t committed_index() const;

private:
    struct Impl;
    Impl* impl_;
};

} // namespace nimbus
