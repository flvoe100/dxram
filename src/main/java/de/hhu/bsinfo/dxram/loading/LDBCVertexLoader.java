package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LDBCVertexLoader implements FileLoader {

    ChunkLocalService m_chunkLocalService;
    ChunkService m_chunkService;


    public LDBCVertexLoader() {

    }

    @Override
    public void readFile(Path p_file, Graph p_graph) {
        //TODO: workaround forcreating custom vertex objects

        try {
            Files.lines(p_file)
                    .mapToLong(line -> Long.parseLong(line.split("\\s")[0]))
                    .forEach(vid -> {
                        Vertex vertex = new SimpleVertex(vid);

                        m_chunkLocalService.createLocal().create(vertex, vid); //TODO
                        m_chunkService.put().put(vertex);
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setLocalService(ChunkLocalService p_chunkLocalService) {
        this.m_chunkLocalService = p_chunkLocalService;
    }

    public void setChunkService(ChunkService service) {
        m_chunkService = service;
    }

}
