#pragma once

#include "meta_coordinator.h"
#include "meta_wal.h"

#include <functional>
#include <string>

namespace nimbus {

using ChunkReplicaDeleteFn = std::function<void(const std::string& addr,
                                               const std::string& chunk_id)>;

std::string resolve_replica_address(const MetaCoordinator& coord,
                                    const std::string& entry);

void cleanup_uncommitted_puts(MetaWal& wal,
                              const MetaCoordinator& coord,
                              const ChunkReplicaDeleteFn& delete_fn);

} // namespace nimbus