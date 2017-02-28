#include "engine/routing_algorithms/mld_shortest_path.hpp"

namespace osrm
{
namespace engine
{
namespace routing_algorithms
{

void MultiLayerDijkstraRouting::RoutingStep(
    const std::shared_ptr<const datafacade::BaseDataFacade> facade,
    SearchEngineData::MultiLayerDijkstraHeap &forward_heap,
    SearchEngineData::MultiLayerDijkstraHeap &reverse_heap,
    util::LevelID highest_level,
    NodeID &middle_node_id,
    EdgeWeight &upper_bound,
    EdgeWeight min_edge_offset,
    const bool forward_direction) const
{
    const auto &partition = facade->GetMultiLevelPartition();
    const auto &cells = facade->GetCellStorage();

    const NodeID node = forward_heap.DeleteMin();
    const EdgeWeight weight = forward_heap.GetKey(node);
    const auto level = forward_heap.GetData(node).level;

    // const auto cell_id = partition.GetCell(level, node);
    std::cout << "RoutingStep " << (forward_direction ? "forward" : "backward") << " for level "
              << (int)level << " " << (level > 0 ? partition.GetCell(level, node) : 0)
              << ", node = " << node << " weight = " << weight << "\n";

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

    if (level == 0)
    {
        const auto upper_cell_id = partition.GetCell(level + 1, node);
        for (const auto edge : facade->GetAdjacentEdgeRange(node))
        {
            const EdgeData &data = facade->GetEdgeData(edge);
            bool forward_directionFlag = (forward_direction ? data.forward : data.backward);
            if (forward_directionFlag)
            {
                const NodeID to = facade->GetTarget(edge);
                const EdgeWeight edge_weight = data.weight;
                const auto next_level = upper_cell_id == partition.GetCell(level + 1, to)
                                            ? util::LevelID{0}
                                            : util::LevelID{1};

                std::cout << "    -> " << to << " weight " << edge_weight << "\n";

                BOOST_ASSERT_MSG(edge_weight > 0, "edge_weight invalid");
                const EdgeWeight to_weight = weight + edge_weight;

                // New Node discovered -> Add to Heap + Node Info Storage
                if (!forward_heap.WasInserted(to))
                {
                    forward_heap.Insert(to, to_weight, {node, next_level});
                    std::cout << "        insert " << to << " key " << to_weight << " {" << node
                              << " " << (int)next_level << "}"
                              << "\n";
                }
                // Found a shorter Path -> Update weight
                else if (to_weight < forward_heap.GetKey(to))
                {
                    // new parent
                    forward_heap.GetData(to).parent = node;
                    forward_heap.GetData(to).level = next_level;
                    forward_heap.DecreaseKey(to, to_weight);
                    std::cout << "        decrease key " << to << " key " << to_weight << " {"
                              << node << " " << (int)next_level << "}"
                              << "\n";
                }
            }
        }
    }
    else
    {
        const auto cell_id = partition.GetCell(level, node);
        auto cell = cells.GetCell(level, cell_id);

        if (forward_direction)
        {
            // Shortcuts in forward direction
            auto destination = cell.GetDestinationNodes().begin();
            for (auto shortcut_weight : cell.GetOutWeight(node))
            {
                BOOST_ASSERT(destination != cell.GetDestinationNodes().end());
                const NodeID to = *destination;
                std::cout << "    -> " << to << " forward shortcut_weight " << shortcut_weight
                          << "\n";
                if (shortcut_weight != INVALID_EDGE_WEIGHT && node != to)
                {
                    const EdgeWeight to_weight = shortcut_weight + weight;
                    if (!forward_heap.WasInserted(to))
                    {
                        forward_heap.Insert(to, to_weight, {node, level});
                        std::cout << "        insert " << to << " key " << to_weight << " {" << node
                                  << " " << (int)level << "}"
                                  << "\n";
                    }
                    else if (to_weight < forward_heap.GetKey(to))
                    {
                        // new parent
                        forward_heap.GetData(to).parent = node;
                        forward_heap.GetData(to).level = level;
                        forward_heap.DecreaseKey(to, to_weight);
                        std::cout << "        decrease key " << to << " key " << to_weight << " {"
                                  << node << " " << (int)level << "}"
                                  << "\n";
                    }
                }
                ++destination;
            }
        }
        else
        {
            // Shortcuts in backward direction
            auto source = cell.GetSourceNodes().begin();
            for (auto shortcut_weight : cell.GetInWeight(node))
            {
                BOOST_ASSERT(source != cell.GetSourceNodes().end());
                const NodeID to = *source;
                std::cout << "    -> " << to << " backward shortcut_weight " << shortcut_weight
                          << "\n";
                if (shortcut_weight != INVALID_EDGE_WEIGHT && node != to)
                {
                    const EdgeWeight to_weight = shortcut_weight + weight;
                    if (!forward_heap.WasInserted(to))
                    {
                        forward_heap.Insert(to, to_weight, {node, level});
                        std::cout << "        insert " << to << " key " << to_weight << " {" << node
                                  << " " << (int)level << "}"
                                  << "\n";
                    }
                    else if (to_weight < forward_heap.GetKey(to))
                    {
                        // new parent
                        forward_heap.GetData(to).parent = node;
                        forward_heap.GetData(to).level = level;
                        forward_heap.DecreaseKey(to, to_weight);
                        std::cout << "        decrease key " << to << " key " << to_weight << " {"
                                  << node << " " << (int)level << "}"
                                  << "\n";
                    }
                }
                ++source;
            }
        }

        // Boundary edges
        // TODO merge with level 0
        for (const auto edge : facade->GetAdjacentEdgeRange(node))
        {
            const EdgeData &data = facade->GetEdgeData(edge);
            bool forward_directionFlag = (forward_direction ? data.forward : data.backward);
            if (forward_directionFlag)
            {
                const NodeID to = facade->GetTarget(edge);
                const EdgeWeight edge_weight = data.weight;
                std::cout << "    -> " << to << " border edge weight " << data.weight
                          << " to cell id " << partition.GetCell(level, to) << "\n";

                if (cell_id != partition.GetCell(level, to))
                {
                    BOOST_ASSERT_MSG(edge_weight > 0, "edge_weight invalid");
                    const EdgeWeight to_weight = weight + edge_weight;

                    // TODO: compute only if needed (optimization only)
                    // TODO: check if `to` is source for data.forward or destination for
                    // data.backward  (optimization only)
                    util::LevelID next_level =
                        std::min(highest_level, partition.GetHighestDifferentLevel(node, to));

                    // New Node discovered -> Add to Heap + Node Info Storage
                    if (!forward_heap.WasInserted(to))
                    {
                        forward_heap.Insert(to, to_weight, {node, next_level});
                        std::cout << "        insert " << to << " key " << to_weight << " {" << node
                                  << " " << (int)next_level << "}"
                                  << "\n";
                    }
                    // Found a shorter Path -> Update weight
                    else if (to_weight < forward_heap.GetKey(to))
                    {
                        // new parent
                        forward_heap.GetData(to).parent = node;
                        forward_heap.GetData(to).level = next_level;
                        forward_heap.DecreaseKey(to, to_weight);
                        std::cout << "        decrease key " << to << " key " << to_weight << " {"
                                  << node << " " << (int)next_level << "}"
                                  << "\n";
                    }
                }
            }
        }
    }
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
    const auto &cells = facade->GetCellStorage();

    std::cout << "forward heap\n";
    if (source_phantom.forward_segment_id.enabled)
    {
        forward_heap.Insert(source_phantom.forward_segment_id.id,
                            -source_phantom.GetForwardWeightPlusOffset(),
                            {source_phantom.forward_segment_id.id, 0});
        std::cout << "  forward_segment " << source_phantom.forward_segment_id.id << " "
                  << " level 1 cell id "
                  << partition.GetCell(1, source_phantom.forward_segment_id.id) << "\n";
    }
    if (source_phantom.reverse_segment_id.enabled)
    {
        forward_heap.Insert(source_phantom.reverse_segment_id.id,
                            -source_phantom.GetReverseWeightPlusOffset(),
                            {source_phantom.reverse_segment_id.id, 0});
        std::cout << "  reverse_segment " << source_phantom.reverse_segment_id.id << " "
                  << " level 1 cell id "
                  << partition.GetCell(1, source_phantom.reverse_segment_id.id) << "\n";
    }

    std::cout << "reverse heap\n";
    if (target_phantom.forward_segment_id.enabled)
    {
        reverse_heap.Insert(target_phantom.forward_segment_id.id,
                            target_phantom.GetForwardWeightPlusOffset(),
                            {target_phantom.forward_segment_id.id, 0});
        std::cout << "  forward_segment " << target_phantom.forward_segment_id.id << " "
                  << " level 1 cell id "
                  << partition.GetCell(1, target_phantom.forward_segment_id.id) << "\n";
    }

    if (target_phantom.reverse_segment_id.enabled)
    {
        reverse_heap.Insert(target_phantom.reverse_segment_id.id,
                            target_phantom.GetReverseWeightPlusOffset(),
                            {target_phantom.reverse_segment_id.id, 0});
        std::cout << "  reverse_segment " << target_phantom.reverse_segment_id.id << " "
                  << " level 1 cell id "
                  << partition.GetCell(1, target_phantom.reverse_segment_id.id) << "\n";
    }

    int weight = INVALID_EDGE_WEIGHT;
    std::vector<NodeID> packed_leg;

    const bool constexpr DO_NOT_FORCE_LOOPS =
        false; // prevents forcing of loops, since offsets are set correctly

    auto get_highest_level = [&partition](const SegmentID &source, const SegmentID &target) {
        if (source.enabled && target.enabled)
            return partition.GetHighestDifferentLevel(source.id, target.id);
        return std::numeric_limits<util::LevelID>::max();
    };

    // TODO: remove check
    EdgeWeight mld_weight = INVALID_EDGE_WEIGHT;

    const auto highest_level =
        std::min(std::min(get_highest_level(source_phantom.forward_segment_id,
                                            target_phantom.forward_segment_id),
                          get_highest_level(source_phantom.forward_segment_id,
                                            target_phantom.reverse_segment_id)),
                 std::min(get_highest_level(source_phantom.reverse_segment_id,
                                            target_phantom.forward_segment_id),
                          get_highest_level(source_phantom.reverse_segment_id,
                                            target_phantom.reverse_segment_id)));

    {
        std::cout << "MLD with #levels "
                  << " " << (int)facade->GetMultiLevelPartition().GetNumberOfLevels() << " "
                  << " highest_level " << highest_level << "\n";

        NodeID middle = SPECIAL_NODEID;
        weight = INVALID_EDGE_WEIGHT;

        // get offset to account for offsets on phantom nodes on compressed edges
        const auto min_edge_offset = std::min(0, forward_heap.MinKey());
        BOOST_ASSERT(min_edge_offset <= 0);
        // we only every insert negative offsets for nodes in the forward heap
        BOOST_ASSERT(reverse_heap.MinKey() >= 0);

        // run two-Target Dijkstra routing step.
        while (0 < (forward_heap.Size() + reverse_heap.Size()))
        {
            if (!forward_heap.Empty())
            {
                RoutingStep(facade,
                            forward_heap,
                            reverse_heap,
                            highest_level,
                            middle,
                            weight,
                            min_edge_offset,
                            true);
            }
            if (!reverse_heap.Empty())
            {
                RoutingStep(facade,
                            reverse_heap,
                            forward_heap,
                            highest_level,
                            middle,
                            weight,
                            min_edge_offset,
                            false);
            }
        }

        // No path found for both target nodes?
        if (weight == INVALID_EDGE_WEIGHT || SPECIAL_NODEID == middle)
        {
            weight = INVALID_EDGE_WEIGHT;
            // return; TODO remove check
        }

        mld_weight = weight;

        std::cout << "MLD weight " << mld_weight << " node " << middle << "\n";
        std::cerr << "MLD weight " << mld_weight << " node " << middle << "\n";

        {
            std::cout << "forward heap\n";
            NodeID node = middle;
            while (forward_heap.GetData(node).parent != node)
            {
                std::cout << "level " << (int)forward_heap.GetData(node).level << " node " << node
                          << " weight " << forward_heap.GetKey(node) << "\n";
                node = forward_heap.GetData(node).parent;
            }
            std::cout << "level " << (int)forward_heap.GetData(node).level << " node " << node
                      << " weight " << forward_heap.GetKey(node) << "\n";
        }
        {
            std::cout << "reverse heap\n";
            NodeID node = middle;
            while (reverse_heap.GetData(node).parent != node)
            {
                std::cout << "level " << (int)reverse_heap.GetData(node).level << " node " << node
                          << " weight " << reverse_heap.GetKey(node) << "\n";
                node = reverse_heap.GetData(node).parent;
            }
            std::cout << "level " << (int)reverse_heap.GetData(node).level << " node " << node
                      << " weight " << reverse_heap.GetKey(node) << "\n";
        }
    }

    // if (facade->GetCoreSize() > 0)
    // {
    //     engine_working_data.InitializeOrClearSecondThreadLocalStorage(facade->GetNumberOfNodes());
    //     QueryHeap &forward_core_heap = *(engine_working_data.forward_heap_2);
    //     QueryHeap &reverse_core_heap = *(engine_working_data.reverse_heap_2);
    //     forward_core_heap.Clear();
    //     reverse_core_heap.Clear();

    //     super::SearchWithCore(facade,
    //                           forward_heap,
    //                           reverse_heap,
    //                           forward_core_heap,
    //                           reverse_core_heap,
    //                           weight,
    //                           packed_leg,
    //                           DO_NOT_FORCE_LOOPS,
    //                           DO_NOT_FORCE_LOOPS);
    // }
    // else
    {
        engine_working_data.InitializeOrClearFirstThreadLocalStorage(facade->GetNumberOfNodes());
        QueryHeap &forward_heap = *(engine_working_data.forward_heap_1);
        QueryHeap &reverse_heap = *(engine_working_data.reverse_heap_1);

        forward_heap.Clear();
        reverse_heap.Clear();

        BOOST_ASSERT(source_phantom.IsValid());
        BOOST_ASSERT(target_phantom.IsValid());

        if (source_phantom.forward_segment_id.enabled)
        {
            forward_heap.Insert(source_phantom.forward_segment_id.id,
                                -source_phantom.GetForwardWeightPlusOffset(),
                                source_phantom.forward_segment_id.id);
        }
        if (source_phantom.reverse_segment_id.enabled)
        {
            forward_heap.Insert(source_phantom.reverse_segment_id.id,
                                -source_phantom.GetReverseWeightPlusOffset(),
                                source_phantom.reverse_segment_id.id);
        }

        if (target_phantom.forward_segment_id.enabled)
        {
            reverse_heap.Insert(target_phantom.forward_segment_id.id,
                                target_phantom.GetForwardWeightPlusOffset(),
                                target_phantom.forward_segment_id.id);
        }

        if (target_phantom.reverse_segment_id.enabled)
        {
            reverse_heap.Insert(target_phantom.reverse_segment_id.id,
                                target_phantom.GetReverseWeightPlusOffset(),
                                target_phantom.reverse_segment_id.id);
        }

        int weight = INVALID_EDGE_WEIGHT;
        std::vector<NodeID> packed_leg;

        super::Search(facade,
                      forward_heap,
                      reverse_heap,
                      weight,
                      packed_leg,
                      DO_NOT_FORCE_LOOPS,
                      DO_NOT_FORCE_LOOPS);

        BOOST_ASSERT(weight == mld_weight);

        // No path found for both target nodes?
        if (INVALID_EDGE_WEIGHT == weight)
        {
            raw_route_data.shortest_path_length = INVALID_EDGE_WEIGHT;
            raw_route_data.alternative_path_length = INVALID_EDGE_WEIGHT;
            return;
        }

        BOOST_ASSERT_MSG(!packed_leg.empty(), "packed path empty");

        raw_route_data.shortest_path_length = weight;
        raw_route_data.unpacked_path_segments.resize(1);
        raw_route_data.source_traversed_in_reverse.push_back(
            (packed_leg.front() != phantom_node_pair.source_phantom.forward_segment_id.id));
        raw_route_data.target_traversed_in_reverse.push_back(
            (packed_leg.back() != phantom_node_pair.target_phantom.forward_segment_id.id));

        super::UnpackPath(facade,
                          packed_leg.begin(),
                          packed_leg.end(),
                          phantom_node_pair,
                          raw_route_data.unpacked_path_segments.front());
    }
}
} // namespace routing_algorithms
} // namespace engine
} // namespace osrm
