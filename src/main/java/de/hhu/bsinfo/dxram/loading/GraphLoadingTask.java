package de.hhu.bsinfo.dxram.loading;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class GraphLoadingTask implements Task {
    @Expose
    private String m_vertexFilePath;

    @Expose
    private String m_edgeFilePath;

    @Expose
    private short m_masterNodeId;

    @Expose
    private int m_lastLineSize;

    @Expose
    private Graph m_graph;

    private static final int QUEUE_CAPACITY = 1_000_000;
    private static final int NUMBER_OF_PRODUCER = 4;

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoadingTask.class.getSimpleName());

    public GraphLoadingTask() {
    }

    public GraphLoadingTask(String p_vertexFilePath, String p_edgeFilePath, short p_masterNodeId, Graph p_graph, int p_lastLineSize) {
        this.m_vertexFilePath = p_vertexFilePath;
        this.m_edgeFilePath = p_edgeFilePath;
        this.m_masterNodeId = p_masterNodeId;
        this.m_graph = p_graph;
        m_lastLineSize = p_lastLineSize;
    }

    @Override
    public int execute(TaskContext p_ctx) {
        ChunkLocalService p_chunkLocalService = p_ctx.getDXRAMServiceAccessor().getService(ChunkLocalService.class);
        ChunkService p_chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        BootService p_bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);

        short nodeID = p_bootService.getNodeID();
        List<Partition> partitions = FilePartitioner.determinePartitions(m_edgeFilePath, m_lastLineSize, p_ctx.getCtxData().getSlaveNodeIds().length, nodeID);
        Partition slavePartition = partitions.get(p_ctx.getCtxData().getSlaveId());
        boolean isLastPartition = p_ctx.getCtxData().getSlaveNodeIds().length == p_ctx.getCtxData().getSlaveId() + 1;
        List<PartitionPartition> pps = FilePartitioner.determinePartitionPartitions(slavePartition, m_edgeFilePath, NUMBER_OF_PRODUCER, isLastPartition, m_lastLineSize);
        System.out.println(slavePartition);

        LOGGER.info("Start loading vertices!");
        HashMap<Long, Vertex> idToVertexMap = new HashMap<>();
        LDBCVertexLoader vertexLoader = new LDBCVertexLoader(p_chunkLocalService, p_chunkService, slavePartition, nodeID, idToVertexMap);
        vertexLoader.readFile(Paths.get(m_vertexFilePath), m_graph);
        LOGGER.info("Finished loading vertices!");
        LOGGER.info("Start loading edges!");
        ArrayBlockingQueue<List<Edge>> p_queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        LOGGER.info("Initializing read threads");
        LDBCEdgePreprocessThread[] producers = new LDBCEdgePreprocessThread[NUMBER_OF_PRODUCER];
        for (int i = 0; i < NUMBER_OF_PRODUCER; i++) {
            producers[i] = new LDBCEdgePreprocessThread(m_edgeFilePath, p_queue, slavePartition, partitions, pps.get(i), m_graph.isIsDirected(), nodeID);
            producers[i].start();
        }
        LOGGER.info("Initialized read threads");
        LOGGER.info("Initializing consume thread");
        DXMemEdgeLoadingThread consumer = new DXMemEdgeLoadingThread(p_queue, partitions, idToVertexMap, p_chunkService, p_chunkLocalService, nodeID);
        consumer.start();
        LOGGER.info("Initialized consume thread");
        try {

            for (int i = 0; i < NUMBER_OF_PRODUCER; i++) {
                producers[i].join();

            }
            consumer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOGGER.info("Finished loading edges!");
        LOGGER.info("Finished loading graph");
        return 0;
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeString(m_vertexFilePath);
        p_exporter.writeString(m_edgeFilePath);
        p_exporter.writeShort(m_masterNodeId);
        p_exporter.writeInt(m_lastLineSize);
        p_exporter.exportObject(m_graph);

    }

    @Override
    public void importObject(Importer p_importer) {
        m_vertexFilePath = p_importer.readString(m_vertexFilePath);
        m_edgeFilePath = p_importer.readString(m_edgeFilePath);
        m_masterNodeId = p_importer.readShort(m_masterNodeId);
        m_lastLineSize = p_importer.readInt(m_lastLineSize);
        if (m_graph == null) {
            m_graph = new Graph();
        }
        m_graph.importObject(p_importer);

    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_vertexFilePath) + ObjectSizeUtil.sizeofString(m_edgeFilePath) + Short.BYTES + m_graph.sizeofObject() + Integer.BYTES;

    }
}
