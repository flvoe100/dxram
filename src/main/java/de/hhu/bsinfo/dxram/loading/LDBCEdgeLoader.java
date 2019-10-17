package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.loading.util.Util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LDBCEdgeLoader extends FileLoader {

    public LDBCEdgeLoader() {
    }

    public LDBCEdgeLoader(ChunkLocalService p_chunkLocalService, ChunkService p_chunkService, short p_nodeID) {
        super(p_chunkLocalService, p_chunkService, p_nodeID);
    }

    @Override
    void readFile(Path p_filePath, GraphLoadingMetaData p_metaData) {
        try {
            long start = System.nanoTime();
            Files.lines(p_filePath)
                    .map(line -> {
                        String[] split = line.split("\\s");
                        long source = Long.parseLong(split[0]);
                        long sink = Long.parseLong(split[1]);

                        return new LongLongPair(source, sink);
                    }).filter(longLongPair -> Util.isInInterval(p_metaData.getStartIntervalOfNode(m_nodeID), p_metaData.getEndIntervalOfNode(m_nodeID), longLongPair.getOne()))
                    .forEach(longLongPair -> {

                        SimpleEdge edge = new SimpleEdge(longLongPair.getOne(), longLongPair.getTwo());

                        m_chunkLocalService.createLocal().create(edge); //TODO
                      //  System.out.println(ChunkID.getLocalID(edge.getID()));
                        m_chunkService.put().put(edge);
                    });

            System.out.println("Loading time: " + (System.nanoTime() - start));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void readFile(Path p_file, Graph p_graph) {

    }

    @Override
    VerticesTaskResponse readVerticesFile(Path p_filePath, short p_masterNodeId) {
        return null;
    }


}
