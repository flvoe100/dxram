package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
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

public class DxGraph implements MessageReceiver {

    private Format m_datasetFormat;
    private GraphLoadingMetaData m_metaData;
    private boolean m_fileForEveryNode;

    private short m_masterNodeID;
    private MasterSlaveComputeService m_masterSlaveService;
    private ChunkService m_chunkService;

    private Graph m_graph;

    private static final Logger LOGGER = LogManager.getFormatterLogger(DxGraph.class.getSimpleName());


    public DxGraph(ServiceProvider p_context, Format p_datasetFormat, boolean p_fileForEveryNode) {
        this.m_datasetFormat = p_datasetFormat;
        this.m_fileForEveryNode = p_fileForEveryNode;
        this.m_masterSlaveService = p_context.getService(MasterSlaveComputeService.class);
        this.m_chunkService = p_context.getService(ChunkService.class);
        ArrayList<Short> p_slaveIDs = m_masterSlaveService.getStatusMaster().getConnectedSlaves();
        this.m_metaData = new GraphLoadingMetaData(p_slaveIDs);
        p_context.getService(ChunkLocalService.class).createLocal().create(m_metaData);
        m_chunkService.put().put(m_metaData);
        this.m_masterNodeID = p_context.getService(BootService.class).getNodeID();
        this.m_graph = new Graph(p_slaveIDs, m_masterNodeID);

        NetworkService p_networkService = p_context.getService(NetworkService.class);
        p_networkService.registerMessageType(DXRAMMessageTypes.GRAPH_LOADING_MESSAGE_TYPE, DxGraphMessageTypes.SUBTYPE_VERTICES_LOADING_TASK_RESPONSE, VerticesTaskResponse.class);
        p_networkService.registerReceiver(DXRAMMessageTypes.GRAPH_LOADING_MESSAGE_TYPE, DxGraphMessageTypes.SUBTYPE_VERTICES_LOADING_TASK_RESPONSE, this);
    }

    public void loadGraph() {
        LOGGER.info("Starting to load");
        if (m_datasetFormat.hasPropertiesFile()) {
            LOGGER.info("Starting to load properties");
            this.loadProperties();
            LOGGER.info("Finished to load properties");
        }
        if (m_datasetFormat.hasVertexFile()) {
            LOGGER.info("Starting to load vertices");
            this.loadVertices();
            LOGGER.info("Starting to load vertices");

        }
        LOGGER.info("Starting to load edges");
        this.loadEdges();
        LOGGER.info("Starting to load edges");

        System.out.println("m_metaData = " + m_metaData);
    }

    private void loadProperties() {
        this.m_datasetFormat.getPropertiesLoader().readFile(this.m_datasetFormat.getPropertiesFilePath(), m_graph);
    }

    private void loadVertices() {
        long start = System.nanoTime();
        VerticesLoadingTask verticesLoadingTask = new VerticesLoadingTask(this.m_datasetFormat.getVertexFilePath().toString(),
                m_datasetFormat.getVertexLoader().getClass().getName(), m_masterNodeID, m_graph);
        TaskScript taskScript = new TaskScript(verticesLoadingTask);
        TaskScriptState state = m_masterSlaveService.submitTaskScript(taskScript);

        while (!state.hasTaskCompleted()) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }
        long end = System.nanoTime();
        System.out.println(String.format("Vertice loading time: %d nanosecs", end - start));
        if (!m_chunkService.put().put(m_metaData)) {
            //error
        }

    }

    private void loadEdges() {
        long start = System.nanoTime();

        EdgesLoadingTask edgesLoadingTask = new EdgesLoadingTask(this.m_datasetFormat.getEdgeFilePath().toString(), m_datasetFormat.getEdgeLoader().getClass().getName(), m_metaData, m_graph);
        TaskScript script = new TaskScript(edgesLoadingTask);
        TaskScriptState state = m_masterSlaveService.submitTaskScript(script);

        while (!state.hasTaskCompleted()) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }
        long end = System.nanoTime();
        System.out.println(String.format("Edge loading time: %d nanosecs", end - start));
    }

    public GraphLoadingMetaData getMetaData() {
        return m_metaData;
    }

    private void incomingVerticesLoadingTaskResponse(VerticesTaskResponse p_response) {
        m_metaData.changeVertexSpaceOfNode(p_response.getNodeID(), p_response.getStartID(), p_response.getEndID());
    }

    @Override
    public void onIncomingMessage(Message p_message) {
        LOGGER.info("Message came in!");
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.GRAPH_LOADING_MESSAGE_TYPE) {
                switch (p_message.getSubtype()) {
                    case DxGraphMessageTypes.SUBTYPE_VERTICES_LOADING_TASK_RESPONSE:
                        incomingVerticesLoadingTaskResponse((VerticesTaskResponse) p_message);
                        break;
                    default:
                        break;
                }
            }
        }
    }
}
