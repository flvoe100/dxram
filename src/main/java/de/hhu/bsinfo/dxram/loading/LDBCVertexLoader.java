package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.net.NetworkService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.LongStream;

public class LDBCVertexLoader extends FileLoader {


    public LDBCVertexLoader() {
    }

    public LDBCVertexLoader(ChunkLocalService p_chunkLocalService, ChunkService p_chunkService, short p_nodeID) {
        super(p_chunkLocalService, p_chunkService, p_nodeID);
    }

    @Override
    void readFile(Path p_filePath, Graph p_graph) {

    }

    @Override
    VerticesTaskResponse readVerticesFile(Path p_filePath, short p_masterNodeId) {
        //TODO: Improving this -> implement own parallel implementation
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        try {
            long start = System.nanoTime();
           Files.lines(p_filePath)
                    .mapToLong(line -> Long.parseLong(line.split("\\s")[0]))
                    .forEach(vid -> {
                        Vertex vertex = new SimpleVertex(vid);


                        m_chunkLocalService.createLocal().create(vertex, vid); //TODO
                        m_chunkService.put().put(vertex);
                    });
            min = Long.parseLong(Files.lines(p_filePath).findFirst().get());
            max = Long.parseLong(Files.lines(p_filePath).reduce((first, second) -> second).orElse("0"));
            System.out.println("Loading time: " + (System.nanoTime() - start  ));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new VerticesTaskResponse(p_masterNodeId, m_nodeID, min, max);
    }

    @Override
    void readFile(Path p_filePath, GraphLoadingMetaData p_metaData) {

    }
}
