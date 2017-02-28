#ifndef MLD_SHORTEST_PATH_HPP
#define MLD_SHORTEST_PATH_HPP

#include <boost/assert.hpp>
#include <iterator>
#include <memory>

#include "engine/datafacade/datafacade_base.hpp"
#include "engine/routing_algorithms/routing_base.hpp"
#include "engine/search_engine_data.hpp"
#include "util/integer_range.hpp"
#include "util/timing_util.hpp"
#include "util/typedefs.hpp"

namespace osrm
{
namespace engine
{
namespace routing_algorithms
{

/// This is a striped down version of the general shortest path algorithm.
/// The general algorithm always computes two queries for each leg. This is only
/// necessary in case of vias, where the directions of the start node is constrainted
/// by the previous route.
/// This variation is only an optimazation for graphs with slow queries, for example
/// not fully contracted graphs.
class MultiLayerDijkstraRouting final : public BasicRoutingInterface
{
    using super = BasicRoutingInterface;
    using QueryHeap = SearchEngineData::QueryHeap;
    SearchEngineData &engine_working_data;

  public:
    MultiLayerDijkstraRouting(SearchEngineData &engine_working_data)
        : engine_working_data(engine_working_data)
    {
    }

    ~MultiLayerDijkstraRouting() {}

    void operator()(const std::shared_ptr<const datafacade::BaseDataFacade> facade,
                    const std::vector<PhantomNodes> &phantom_nodes_vector,
                    InternalRouteResult &raw_route_data) const;

    void RoutingStep(const std::shared_ptr<const datafacade::BaseDataFacade> facade,
                     SearchEngineData::MultiLayerDijkstraHeap &forward_heap,
                     SearchEngineData::MultiLayerDijkstraHeap &reverse_heap,
                     util::LevelID highest_level,
                     NodeID &middle_node_id,
                     EdgeWeight &upper_bound,
                     EdgeWeight min_edge_offset,
                     const bool forward_direction) const;
};

} // namespace routing_algorithms
} // namespace engine
} // namespace osrm

#endif /* MLD_SHORTEST_PATH_HPP */
