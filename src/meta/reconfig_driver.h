#pragma once
#include "meta_coordinator.h"
#include "meta_replication.h"
#include "placement.h"
#include <string>
#include <vector>
#include <functional>

namespace nimbus {

class ReconfigDriver {
public:
    ReconfigDriver(MetaCoordinator& coord,
                   MetaReplication& repl,
                   const Placement&  placement);

    bool reconfig(const std::string& chunk_id, int new_rf,
                  const std::string& must_include_node_id = "",
                  const std::string& must_exclude_node_id = "");

private:
    bool copy_to_new_replicas(const std::string& chunk_id,
                               uint64_t version,
                               const std::vector<std::string>& src_nodes,
                               const std::vector<std::string>& new_nodes);

    bool verify_replicas(const std::string& chunk_id,
                          uint64_t required_version,
                          const std::vector<std::string>& nodes);

    MetaCoordinator& coord_;
    MetaReplication& repl_;
    const Placement& placement_;
};

} // namespace nimbus
