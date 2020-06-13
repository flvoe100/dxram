package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

public class VertexLoadingConsumer implements Runnable {
    private BlockingDeque<List<Long>> m_vIDsQueue;
    private ChunkLocalService m_chunkLocalService;
    private ChunkService m_chunkService;
    private HashMap<Long, Vertex> m_idToVertexMap;
    private static final Logger LOGGER = LogManager.getFormatterLogger(VertexLoadingConsumer.class.getSimpleName());

    public VertexLoadingConsumer(BlockingDeque<List<Long>> vidsQueue, ChunkLocalService m_chunkLocalService, ChunkService m_chunkService, HashMap<Long, Vertex> p_idToVertexMap) {
        this.m_vIDsQueue = vidsQueue;
        this.m_chunkLocalService = m_chunkLocalService;
        this.m_chunkService = m_chunkService;
        this.m_idToVertexMap = p_idToVertexMap;
    }


    @Override
    public void run() {

        try {
            System.out.println("Start creating vertices");
            int processedVid = 0;
            long[] p_cids;
            Vertex[] vertices;
            Vertex v = new Vertex();
            while (true) {

                List<Long> ids = m_vIDsQueue.poll(3, TimeUnit.SECONDS);

                if (ids == null) {
                    break;
                }

                p_cids = new long[ids.size()];


                int successfulCreates = m_chunkLocalService.createLocal().create(p_cids, ids.size(), v.sizeofObject(), true, false);
                if (successfulCreates != ids.size()) {
                    LOGGER.error("Error: %d vertices were not created", ids.size() - successfulCreates);
                }

                vertices = new Vertex[p_cids.length];
                for (int i = 0; i < p_cids.length; i++) {
                    v = new Vertex(p_cids[i]);
                    vertices[i] = v;
                }
                int successfulPuts = m_chunkService.put().put(vertices);
                processedVid += successfulPuts;
                if (successfulPuts != ids.size()) {
                    LOGGER.error("Error: %d vertices were not put", ids.size() - successfulPuts);
                }

                for (int i = 0; i < vertices.length; i++) {
                    m_idToVertexMap.put(ids.get(i), vertices[i]);
                }
            }
            LOGGER.info("%d vertices created and put", processedVid);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
