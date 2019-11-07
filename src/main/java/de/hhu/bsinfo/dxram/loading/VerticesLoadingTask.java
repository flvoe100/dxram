package de.hhu.bsinfo.dxram.loading;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.nio.file.Paths;

public class VerticesLoadingTask implements Task {
    @Expose
    private String m_vertexFilePath;


    @Expose
    private String m_loaderClassName;

    @Expose
    private short m_masterNodeId;

    @Expose
    private Graph m_graph;


    public VerticesLoadingTask() {
    }

    public VerticesLoadingTask(String p_vertexFilePath, String p_loaderClassName, short p_masterNodeId, Graph p_graph) {
        this.m_vertexFilePath = p_vertexFilePath;
        this.m_loaderClassName = p_loaderClassName;
        this.m_masterNodeId = p_masterNodeId;
        this.m_graph = p_graph;
    }

    @Override
    public int execute(TaskContext p_ctx) {

        FileLoader loader = null;
        ChunkLocalService p_chunkLocalService = p_ctx.getDXRAMServiceAccessor().getService(ChunkLocalService.class);
        ChunkService p_chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        BootService p_bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);
        NetworkService p_networkService = p_ctx.getDXRAMServiceAccessor().getService(NetworkService.class);

        short nodeID = p_bootService.getNodeID();

        if (m_loaderClassName.equals(LDBCVertexLoader.class.getName())) {
            loader = new LDBCVertexLoader(m_graph.getNumberOfVerticesOfSlave(nodeID), p_chunkLocalService, p_chunkService, nodeID);
        }
        VerticesTaskResponse response = loader.readVerticesFile(Paths.get(m_vertexFilePath), m_graph);
        try {
            p_networkService.sendMessage(response);
        } catch (NetworkException e) {
            e.printStackTrace();
        }

        return 0;
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeString(m_vertexFilePath);
        p_exporter.writeString(m_loaderClassName);
        p_exporter.writeShort(m_masterNodeId);
        p_exporter.exportObject(m_graph);

    }

    @Override
    public void importObject(Importer p_importer) {
        m_vertexFilePath = p_importer.readString(m_vertexFilePath);
        m_loaderClassName = p_importer.readString(m_loaderClassName);
        m_masterNodeId = p_importer.readShort(m_masterNodeId);
        if (m_graph == null) {
            m_graph = new Graph();
        }
        m_graph.importObject(p_importer);

    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_vertexFilePath) + ObjectSizeUtil.sizeofString(m_loaderClassName) + Short.BYTES + m_graph.sizeofObject();

    }
}
