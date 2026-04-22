#pragma once
#include "meta_coordinator.h"
#include "meta_wal.h"
#include <string>
#include <cstdint>

namespace nimbus {

class MetaSnapshot {
public:
    MetaSnapshot(MetaCoordinator& coord, MetaWal& wal,
                 const std::string& data_dir);

    std::pair<std::string, uint64_t> take_snapshot();

    uint64_t install_snapshot(const std::string& snapshot_data);

private:
    MetaCoordinator& coord_;
    MetaWal&         wal_;
    std::string      data_dir_;
};

} // namespace nimbus
