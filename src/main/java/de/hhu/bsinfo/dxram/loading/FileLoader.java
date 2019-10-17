package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.nio.file.Path;

public abstract class FileLoader {

    ChunkLocalService m_chunkLocalService;
    ChunkService m_chunkService;
    short m_nodeID;


    public FileLoader() {
    }

    public FileLoader(ChunkLocalService p_chunkLocalService, ChunkService p_chunkService, short p_nodeID) {
        this.m_chunkLocalService = p_chunkLocalService;
        this.m_chunkService = p_chunkService;
        this.m_nodeID = p_nodeID;
    }


    abstract void readFile(Path p_filePath, Graph p_graph);

   abstract VerticesTaskResponse readVerticesFile(Path p_filePath, short p_masterNodeId);

   abstract void readFile(Path p_filePath, GraphLoadingMetaData p_metaData);
}
