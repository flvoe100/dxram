package de.hhu.bsinfo.dxram.loading;


import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.ServiceProvider;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.TaskScriptState;
import de.hhu.bsinfo.dxram.ms.script.TaskScript;
import de.hhu.bsinfo.dxram.net.NetworkService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

public class DxGraph {

    private Format m_datasetFormat;
    private GraphLoadingMetaData m_metaData;
    private short m_masterNodeID;
    private MasterSlaveComputeService m_masterSlaveService;
    private ChunkService m_chunkService;
    private int m_lastLineSize;
    private Graph m_graph;

    private static final Logger LOGGER = LogManager.getFormatterLogger(DxGraph.class.getSimpleName());


    public DxGraph(ServiceProvider p_context, Format p_datasetFormat, int p_lastLineSize) {
        this.m_lastLineSize = p_lastLineSize;
        this.m_datasetFormat = p_datasetFormat;
        this.m_masterSlaveService = p_context.getService(MasterSlaveComputeService.class);
        this.m_chunkService = p_context.getService(ChunkService.class);
        ArrayList<Short> p_slaveIDs = m_masterSlaveService.getStatusMaster().getConnectedSlaves();
        this.m_metaData = new GraphLoadingMetaData(p_slaveIDs);
        this.m_masterNodeID = p_context.getService(BootService.class).getNodeID();
        this.m_graph = new Graph(p_slaveIDs, m_masterNodeID);

    }

    public void loadGraph() {
        LOGGER.info("Starting to load");
        if (m_datasetFormat.hasPropertiesFile()) {
            LOGGER.info("Starting to load properties");
            this.loadProperties();
            LOGGER.info("Finished to load properties");
        }
        LOGGER.info("Starting to load graph");
        GraphLoadingTask graphLoadingTask = new GraphLoadingTask(m_datasetFormat.getVertexFilePath().toString(), m_datasetFormat.getEdgeFilePath().toString(), m_masterNodeID, m_graph, m_lastLineSize);
        TaskScript taskScript = new TaskScript(graphLoadingTask);
        LOGGER.info("Starting to load graph");
        long start = System.nanoTime();
        TaskScriptState state = m_masterSlaveService.submitTaskScript(taskScript);

        while (!state.hasTaskCompleted()) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }

        long end = System.nanoTime();
        System.out.println(String.format("Vertice loading time: %d nanosecs", end - start));
    }

    private void loadProperties() {
        this.m_datasetFormat.getPropertiesLoader().readFile(this.m_datasetFormat.getPropertiesFilePath(), m_graph);
    }

}
