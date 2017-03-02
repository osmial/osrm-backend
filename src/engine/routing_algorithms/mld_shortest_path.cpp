#include "engine/routing_algorithms/mld_shortest_path.hpp"
#include "util/for_each_pair.hpp"

#include <valgrind/callgrind.h>

namespace osrm
{
namespace engine
{
namespace routing_algorithms
{

// TODO: remove output
// TODO: reduce code duplication
void MultiLayerDijkstraRouting::RoutingStep(
    const std::shared_ptr<const datafacade::BaseDataFacade> facade,
    SearchEngineData::MultiLayerDijkstraHeap &forward_heap,
    SearchEngineData::MultiLayerDijkstraHeap &reverse_heap,
    util::LevelID highest_level,
    util::CellID highest_level_parent_cell,
    NodeID &middle_node_id,
    EdgeWeight &upper_bound,
    EdgeWeight min_edge_offset,
    const bool forward_direction) const
{
    const auto &partition = facade->GetMultiLevelPartition();
    const auto &cells = facade->GetCellStorage();

    // TODO: highly inefficient and takes 20-40% percent of time
    auto get_highest_level = [&](const NodeID node) {
        // Get highest possible hierarchy level:
        // if forward_direction is true
        //   node is a source on the returned level and an internal node on the next level
        // if forward_direction is true
        //   node is a destination on the returned level and an internal node on the next level
        util::LevelID level = 0;
        if (forward_direction)
        {
            while (level < highest_level &&
                   cells.GetCell(level + 1, partition.GetCell(level + 1, node)).IsSourceNode(node))
                ++level;
        }
        else
        {
            while (level < highest_level &&
                   cells.GetCell(level + 1, partition.GetCell(level + 1, node))
                       .IsDestinationNode(node))
                ++level;
        }
        return level;
    };

    const auto node = forward_heap.DeleteMin();
    const auto weight = forward_heap.GetKey(node);

    if (reverse_heap.WasInserted(node))
    {
        const EdgeWeight new_weight = reverse_heap.GetKey(node) + weight;
        if (new_weight < upper_bound)
        {
            BOOST_ASSERT(new_weight >= 0);

            middle_node_id = node;
            upper_bound = new_weight;
        }
    }

    // make sure we don't terminate too early if we initialize the weight
    // for the nodes in the forward heap with the forward/reverse offset
    BOOST_ASSERT(min_edge_offset <= 0);
    if (weight + min_edge_offset > upper_bound)
    {
        forward_heap.DeleteAll();
        return;
    }

    // Boundary edges
    for (const auto edge : facade->GetAdjacentEdgeRange(node))
    {
        const EdgeData &data = facade->GetEdgeData(edge);
        bool forward_directionFlag = (forward_direction ? data.forward : data.backward);
        if (forward_directionFlag)
        {
            const NodeID to = facade->GetTarget(edge);

            // Condition for a boundary edge node -> to on the current level
            // and condition to be in the same highest_level_parent cell on highest_level + 1 level
            const auto from_level = forward_heap.GetData(node).edge_level;
            // Get highest possible hierarchy level
            // const auto to_level = get_highest_level(to);

            if ( // Routing is unrestricted or restricted to the highest level cell
                (highest_level_parent_cell == util::INVALID_CELL_ID ||
                 highest_level_parent_cell == partition.GetCell(highest_level + 1, to)) &&
                // "Route-inside-parent-cell" condition
                (partition.GetCell(from_level + 1, node) == partition.GetCell(from_level + 1, to)
                 // "Always-go-up-at-boundary" condition
                 ||
                 get_highest_level(to) > from_level))
            {
                BOOST_ASSERT_MSG(data.weight > 0, "edge_weight invalid");
                const EdgeWeight to_weight = weight + data.weight;

                // New Node discovered -> Add to Heap + Node Info Storage
                if (!forward_heap.WasInserted(to))
                {
                    forward_heap.Insert(to, to_weight, {node, 0});
                }
                // Found a shorter Path -> Update weight
                else if (to_weight < forward_heap.GetKey(to))
                {
                    // new parent
                    forward_heap.GetData(to).parent = node;
                    forward_heap.GetData(to).edge_level = 0; // the base graph edge has level 0
                    forward_heap.DecreaseKey(to, to_weight);
                }
            }
        }
    }

    // Overlay edges
    if (forward_heap.GetData(node).edge_level == 0)
    {
        const auto level = get_highest_level(node);
        if (level >= 1)
        {
            if (forward_direction)
            {
                // Shortcuts in forward direction
                const auto &cell = cells.GetCell(level, partition.GetCell(level, node));
                auto destination = cell.GetDestinationNodes().begin();
                for (auto shortcut_weight : cell.GetOutWeight(node))
                {
                    BOOST_ASSERT(destination != cell.GetDestinationNodes().end());
                    const NodeID to = *destination;
                    if (shortcut_weight != INVALID_EDGE_WEIGHT && node != to)
                    {
                        const EdgeWeight to_weight = weight + shortcut_weight;
                        if (!forward_heap.WasInserted(to))
                        {
                            forward_heap.Insert(to, to_weight, {node, level});
                        }
                        else if (to_weight < forward_heap.GetKey(to))
                        {
                            // new parent
                            forward_heap.GetData(to).parent = node;
                            forward_heap.GetData(to).edge_level = level;
                            forward_heap.DecreaseKey(to, to_weight);
                        }
                    }
                    ++destination;
                }
            }
            else
            {
                // Shortcuts in backward direction
                const auto &cell = cells.GetCell(level, partition.GetCell(level, node));
                auto source = cell.GetSourceNodes().begin();
                for (auto shortcut_weight : cell.GetInWeight(node))
                {
                    BOOST_ASSERT(source != cell.GetSourceNodes().end());
                    const NodeID to = *source;
                    if (shortcut_weight != INVALID_EDGE_WEIGHT && node != to)
                    {
                        const EdgeWeight to_weight = weight + shortcut_weight;
                        if (!forward_heap.WasInserted(to))
                        {
                            forward_heap.Insert(to, to_weight, {node, level});
                        }
                        else if (to_weight < forward_heap.GetKey(to))
                        {
                            // new parent
                            forward_heap.GetData(to).parent = node;
                            forward_heap.GetData(to).edge_level = level;
                            forward_heap.DecreaseKey(to, to_weight);
                        }
                    }
                    ++source;
                }
            }
        }
    }
}

std::vector<NodeID> MultiLayerDijkstraRouting::UnpackSubPath(
    const std::shared_ptr<const datafacade::BaseDataFacade> facade,
    SearchEngineData::MultiLayerDijkstraHeap &forward_heap,
    SearchEngineData::MultiLayerDijkstraHeap &reverse_heap,
    util::LevelID highest_level,
    util::CellID highest_level_parent_cell,
    EdgeWeight &mld_weight,
    const std::string &padding) const
{
    const auto &partition = facade->GetMultiLevelPartition();

    // This part is completely broken and must be redesigned
    // -> UnpackSubPath must be a pure function without external state
    // of forward_heap and reverse_heap that also contain source and target points
    // At the moment it is used only for customization manual tests

    // run two-Target Dijkstra routing step.
    NodeID middle = SPECIAL_NODEID;
    EdgeWeight weight = INVALID_EDGE_WEIGHT;

    TIMER_START(mld_routing);
    while (0 < (forward_heap.Size() + reverse_heap.Size()))
    {
        if (!forward_heap.Empty())
        {
            RoutingStep(facade,
                        forward_heap,
                        reverse_heap,
                        highest_level,
                        highest_level_parent_cell,
                        middle,
                        weight,
                        0,
                        true);
        }
        if (!reverse_heap.Empty())
        {
            RoutingStep(facade,
                        reverse_heap,
                        forward_heap,
                        highest_level,
                        highest_level_parent_cell,
                        middle,
                        weight,
                        0,
                        false);
        }
    }
    TIMER_STOP(mld_routing);
    util::Log() << padding << "MLD routing took " << TIMER_SEC(mld_routing) << " seconds";

    // No path found for both target nodes?
    if (weight == INVALID_EDGE_WEIGHT || SPECIAL_NODEID == middle)
    {
        weight = INVALID_EDGE_WEIGHT;
        return std::vector<NodeID>();
    }

    // save upward weight
    if (mld_weight == INVALID_EDGE_WEIGHT)
        mld_weight = weight;

    if (forward_heap.GetData(middle).parent == middle &&
        reverse_heap.GetData(middle).parent == middle)
    { // Single-edge path
        return std::vector<NodeID>(1, middle);
    }

    // Get packed path
    std::vector<std::pair<util::LevelID, NodeID>> packed_path;
    NodeID current_node = middle, parent_node = forward_heap.GetData(middle).parent;
    while (parent_node != current_node)
    {
        packed_path.push_back({forward_heap.GetData(current_node).edge_level, current_node});
        current_node = parent_node;
        parent_node = forward_heap.GetData(parent_node).parent;
    }
    packed_path.push_back({forward_heap.GetData(current_node).edge_level, current_node});
    std::reverse(std::begin(packed_path), std::end(packed_path));

    current_node = middle, parent_node = reverse_heap.GetData(middle).parent;
    while (parent_node != current_node)
    {
        packed_path.push_back({reverse_heap.GetData(current_node).edge_level, parent_node});
        current_node = parent_node;
        parent_node = reverse_heap.GetData(parent_node).parent;
    }

    std::vector<NodeID> unpacked_path;

    TIMER_START(mld_path_unpacking);
    util::for_each_pair(
        packed_path,
        [this, &facade, &forward_heap, &reverse_heap, &partition, &unpacked_path, &padding](
            const auto &source, const auto &target) {
            if (unpacked_path.empty())
                unpacked_path.push_back(source.second);

            if (target.first == 0)
            { // a base graph edge (level 0 edge or boundary edge)
                unpacked_path.push_back(target.second);
            }
            else
            { // an overlay graph edge
                auto parent_cell_id = partition.GetCell(target.first, target.second);
                auto sublevel = target.first - 1;

                // Here heaps can be reused, let's go deeper!
                forward_heap.Clear();
                reverse_heap.Clear();
                forward_heap.Insert(source.second, 0, {source.second, 0});
                reverse_heap.Insert(target.second, 0, {target.second, 0});

                EdgeWeight weight = INVALID_EDGE_WEIGHT;
                const auto &subpath = this->UnpackSubPath(facade,
                                                          forward_heap,
                                                          reverse_heap,
                                                          sublevel,
                                                          parent_cell_id,
                                                          weight,
                                                          padding + "  ");
                BOOST_ASSERT(subpath.size() > 1);
                unpacked_path.insert(unpacked_path.end(), subpath.begin() + 1, subpath.end());
            }
        });
    TIMER_STOP(mld_path_unpacking);
    util::Log() << padding << "MLD path unpacking took " << TIMER_SEC(mld_path_unpacking)
                << " seconds";

    return std::move(unpacked_path);
}

/// This is a striped down version of the general shortest path algorithm.
/// The general algorithm always computes two queries for each leg. This is only
/// necessary in case of vias, where the directions of the start node is constrainted
/// by the previous route.
/// This variation is only an optimazation for graphs with slow queries, for example
/// not fully contracted graphs.
void MultiLayerDijkstraRouting::
operator()(const std::shared_ptr<const datafacade::BaseDataFacade> facade,
           const std::vector<PhantomNodes> &phantom_nodes_vector,
           InternalRouteResult &raw_route_data) const
{
    // Get weight to next pair of target nodes.
    BOOST_ASSERT_MSG(1 == phantom_nodes_vector.size(),
                     "Direct Shortest Path Query only accepts a single source and target pair. "
                     "Multiple ones have been specified.");
    const auto &phantom_node_pair = phantom_nodes_vector.front();
    const auto &source_phantom = phantom_node_pair.source_phantom;
    const auto &target_phantom = phantom_node_pair.target_phantom;

    engine_working_data.InitializeOrClearMultiLayerDijkstraThreadLocalStorage(
        facade->GetNumberOfNodes());
    auto &forward_heap = *(engine_working_data.mld_forward_heap);
    auto &reverse_heap = *(engine_working_data.mld_reverse_heap);
    forward_heap.Clear();
    reverse_heap.Clear();

    BOOST_ASSERT(source_phantom.IsValid());
    BOOST_ASSERT(target_phantom.IsValid());

    const auto &partition = facade->GetMultiLevelPartition();

    auto get_highest_level = [&partition](const SegmentID &source, const SegmentID &target) {
        if (source.enabled && target.enabled)
            return partition.GetHighestDifferentLevel(source.id, target.id);
        return std::numeric_limits<util::LevelID>::max();
    };

    const auto highest_level =
        std::min(std::min(get_highest_level(source_phantom.forward_segment_id,
                                            target_phantom.forward_segment_id),
                          get_highest_level(source_phantom.forward_segment_id,
                                            target_phantom.reverse_segment_id)),
                 std::min(get_highest_level(source_phantom.reverse_segment_id,
                                            target_phantom.forward_segment_id),
                          get_highest_level(source_phantom.reverse_segment_id,
                                            target_phantom.reverse_segment_id)));

    if (source_phantom.forward_segment_id.enabled)
    {
        forward_heap.Insert(source_phantom.forward_segment_id.id,
                            -source_phantom.GetForwardWeightPlusOffset(),
                            {source_phantom.forward_segment_id.id, 0});
    }
    if (source_phantom.reverse_segment_id.enabled)
    {
        forward_heap.Insert(source_phantom.reverse_segment_id.id,
                            -source_phantom.GetReverseWeightPlusOffset(),
                            {source_phantom.reverse_segment_id.id, 0});
    }

    if (target_phantom.forward_segment_id.enabled)
    {
        reverse_heap.Insert(target_phantom.forward_segment_id.id,
                            target_phantom.GetForwardWeightPlusOffset(),
                            {target_phantom.forward_segment_id.id, 0});
    }

    if (target_phantom.reverse_segment_id.enabled)
    {
        reverse_heap.Insert(target_phantom.reverse_segment_id.id,
                            target_phantom.GetReverseWeightPlusOffset(),
                            {target_phantom.reverse_segment_id.id, 0});
    }

    const bool constexpr DO_NOT_FORCE_LOOPS =
        false; // prevents forcing of loops, since offsets are set correctly

    EdgeWeight mld_weight = INVALID_EDGE_WEIGHT;

    CALLGRIND_START_INSTRUMENTATION;
    auto unpacked_path = UnpackSubPath(
        facade, forward_heap, reverse_heap, highest_level, util::INVALID_CELL_ID, mld_weight);
    CALLGRIND_STOP_INSTRUMENTATION;

    if (unpacked_path.empty())
    {
        raw_route_data.shortest_path_length = INVALID_EDGE_WEIGHT;
        raw_route_data.alternative_path_length = INVALID_EDGE_WEIGHT;
        return;
    }

    raw_route_data.shortest_path_length = mld_weight;
    raw_route_data.unpacked_path_segments.resize(1);
    raw_route_data.source_traversed_in_reverse.push_back(
        (unpacked_path.front() != phantom_node_pair.source_phantom.forward_segment_id.id));
    raw_route_data.target_traversed_in_reverse.push_back(
        (unpacked_path.back() != phantom_node_pair.target_phantom.forward_segment_id.id));

    super::UnpackPath(
        facade, unpacked_path, phantom_node_pair, raw_route_data.unpacked_path_segments.front());
}
} // namespace routing_algorithms
} // namespace engine
} // namespace osrm
