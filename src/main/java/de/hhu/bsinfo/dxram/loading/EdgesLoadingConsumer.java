package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;

public class EdgesLoadingConsumer implements Runnable {
    private BlockingDeque<Pair<Long>> m_vIDsQueue;
    private ChunkLocalService m_chunkLocalService;
    private ChunkService m_chunkService;
    private CountDownLatch m_countDownLatch;

    private static final Logger LOGGER = LogManager.getFormatterLogger(VertexLoadingConsumer.class.getSimpleName());
    final int VERTEX_PACKAGE_SIZE = 10_000;

    public EdgesLoadingConsumer(BlockingDeque<Pair<Long>> p_vIDsQueue, CountDownLatch p_numberOfEdges, ChunkLocalService p_chunkLocalService, ChunkService p_chunkService) {
        this.m_vIDsQueue = p_vIDsQueue;
        this.m_chunkLocalService = p_chunkLocalService;
        this.m_chunkService = p_chunkService;
        this.m_countDownLatch = p_numberOfEdges;
    }

    @Override
    public void run() {
        try {
            System.out.println("Start creating edges");
            int processedVid = 0;
            long[] p_cids;
            SimpleEdge e = new SimpleEdge();
            SimpleEdge[] edges;
            while (m_countDownLatch.getCount() != 0) {
                int nextPackageSize = m_countDownLatch.getCount() - VERTEX_PACKAGE_SIZE < 0 ? (int) m_countDownLatch.getCount() : VERTEX_PACKAGE_SIZE;
                p_cids = new long[nextPackageSize];
                m_chunkService.createLocal().writeRingBuffer();
                int successfulCreates = m_chunkLocalService.createLocal().create(p_cids, nextPackageSize, e.sizeofObject(), false);

                if (successfulCreates != nextPackageSize) {
                    LOGGER.error("Error: %d vertices were not created", VERTEX_PACKAGE_SIZE - successfulCreates);
                }

                edges = new SimpleEdge[nextPackageSize];

                for (int i = 0; i < nextPackageSize; i++) {
                    Pair<Long> p_vIDs = m_vIDsQueue.take();


                    e = new SimpleEdge(p_cids[i], p_vIDs.getTo());
                    edges[i] = e;
                    processedVid++;
                    m_countDownLatch.countDown();

                    if (processedVid % 10_000 == 0) {
                        LOGGER.info("Took %d Edges", processedVid);
                    }
                }

                int successfulPuts = m_chunkService.put().put(edges);
                LOGGER.info("Put %d Edges", nextPackageSize);
                System.out.println(String.format("Put %d Edges", nextPackageSize));
                if (successfulPuts != nextPackageSize) {
                    LOGGER.error("Error: %d vertices were not put", VERTEX_PACKAGE_SIZE - successfulPuts);
                }
            }
            LOGGER.info("%d vertices created and put", processedVid);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
