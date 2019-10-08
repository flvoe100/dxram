package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LDBCVertexLoader implements FileLoader {

    private ChunkLocalService m_chunkLocalService;
    private ChunkService m_chunkService;

    @Override
    public void readFile(Path p_file, Graph p_graph) {
        //TODO: workaround forcreating custom vertex objects
        try {
            Files.lines(p_file)
                    .mapToLong(line -> Long.parseLong(line.split("\\s")[0]))
                    .forEach(vid -> {
                        Vertex vertex = new SimpleVertex();
                        m_chunkLocalService.createLocal().create(vertex); //TODO
                        m_chunkService.put().put(vertex);
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setChunkLocalService(ChunkLocalService p_chunkLocalService) {
        this.m_chunkLocalService = p_chunkLocalService;
    }

    public void setChunkService(ChunkService p_chunkService) {
        this.m_chunkService = p_chunkService;
    }
}
