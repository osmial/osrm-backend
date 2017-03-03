#include "customizer/customizer.hpp"
#include "customizer/cell_customizer.hpp"
#include "partition/edge_based_graph_reader.hpp"

#include "util/cell_storage.hpp"
#include "util/log.hpp"
#include "util/multi_level_partition.hpp"
#include "util/timing_util.hpp"

namespace osrm
{
namespace customize
{

template <typename Graph, typename Partition, typename CellStorage>
void CellStorageStatistics(const Graph &graph,
                           const Partition &partition,
                           const CellStorage &storage)
{
    util::Log() << "Cells statistics per level [source/destination/all]";

    for (std::size_t level = 1; level < partition.GetNumberOfLevels(); ++level)
    {
        std::unordered_map<util::CellID, std::size_t> cell_nodes;
        for (auto node : util::irange(0u, graph.GetNumberOfNodes()))
        {
            ++cell_nodes[partition.GetCell(level, node)];
        }

        std::stringstream sstr;
        std::size_t source = 0, destination = 0, total = 0;
        for (util::CellID cell_id = 0; cell_id < partition.GetNumberOfCells(level); ++cell_id)
        {
            BOOST_ASSERT(cell_nodes.count(cell_id) > 0);

            auto cell = storage.GetCell(level, cell_id);
            sstr << " " << cell_id << " " << cell.GetSourceNodes().size() << "/"
                 << cell.GetDestinationNodes().size() << "/" << cell_nodes[cell_id];
            source += cell.GetSourceNodes().size();
            destination += cell.GetSourceNodes().size();
            total += cell_nodes[cell_id];
        }

        util::Log() << "Level " << level << " #cells " << cell_nodes.size()
                    //<< sstr.str()
                    << " average source nodes " << source << " of " << total << " ("
                    << (100. * source / total) << "%)"
                    << " average destination nodes " << destination << " of " << total << " ("
                    << (100. * destination / total) << "%)";
    }
}

int Customizer::Run(const CustomizationConfig &config)
{
    TIMER_START(loading_data);
    auto edge_based_graph = partition::LoadEdgeBasedGraph(config.edge_based_graph_path.string());
    util::Log() << "Loaded edge based graph for mapping partition ids: "
                << edge_based_graph->GetNumberOfEdges() << " edges, "
                << edge_based_graph->GetNumberOfNodes() << " nodes";

    osrm::util::PackedMultiLevelPartition<false> mlp;
    mlp.Read(config.mld_partition_path);
    // here start with fresh storage, TODO check caching
    osrm::util::CellStorage<false> storage(mlp, *edge_based_graph);
    TIMER_STOP(loading_data);
    util::Log() << "Loading partition data took " << TIMER_SEC(loading_data) << " seconds";

    TIMER_START(cell_customize);
    osrm::customizer::CellCustomizer customizer(mlp);
    customizer.Customize(*edge_based_graph, storage);
    TIMER_STOP(cell_customize);
    util::Log() << "Cells customization took " << TIMER_SEC(cell_customize) << " seconds";

    TIMER_START(writing_mld_data);
    storage.Write(config.mld_storage_path);
    TIMER_STOP(writing_mld_data);
    util::Log() << "MLD customization writing took " << TIMER_SEC(writing_mld_data) << " seconds";

    return 0;
}

} // namespace partition
} // namespace osrm
