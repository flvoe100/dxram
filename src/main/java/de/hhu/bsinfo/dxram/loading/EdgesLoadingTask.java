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

import java.nio.file.Paths;

public class EdgesLoadingTask implements Task {
    @Expose
    private String m_edgeFilePath;

    @Expose
    private String m_edgeLoaderName;

    @Expose
    private GraphLoadingMetaData m_metaData;

    @Expose
    private Graph m_graph;

    public EdgesLoadingTask() {
    }

    public EdgesLoadingTask(String p_edgeFilePath, String p_edgeLoaderName, GraphLoadingMetaData p_metaData, Graph p_graph) {
        this.m_edgeFilePath = p_edgeFilePath;
        this.m_edgeLoaderName = p_edgeLoaderName;
        this.m_metaData = p_metaData;
        this.m_graph = p_graph;
    }


    @Override
    public int execute(TaskContext p_ctx) {
        FileLoader loader = null;
        System.out.println("LOADING EDGEEEES");
        ChunkLocalService p_chunkLocalService = p_ctx.getDXRAMServiceAccessor().getService(ChunkLocalService.class);
        ChunkService p_chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        BootService p_bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);

        short nodeID = p_bootService.getNodeID();

        if (m_edgeLoaderName.equals(LDBCEdgeLoader.class.getName())) {
            loader = new LDBCEdgeLoader(m_graph.getNumberOfEdgesOfSlave(nodeID), m_metaData, p_chunkLocalService, p_chunkService, nodeID);
        }
        loader.readFile(Paths.get(m_edgeFilePath), m_graph);


        return 0;
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeString(m_edgeFilePath);
        p_exporter.writeString(m_edgeLoaderName);
        p_exporter.exportObject(m_metaData);
        p_exporter.exportObject(m_graph);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_edgeFilePath = p_importer.readString(m_edgeFilePath);
        m_edgeLoaderName = p_importer.readString(m_edgeLoaderName);
        if (m_metaData == null) {
            m_metaData = new GraphLoadingMetaData();
        }
        m_metaData.importObject(p_importer);
        if (m_graph == null) {
            m_graph = new Graph();
        }
        m_graph.importObject(p_importer);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_edgeFilePath) + ObjectSizeUtil.sizeofString(m_edgeLoaderName)
                + m_metaData.sizeofObject() + m_graph.sizeofObject();
    }
}
