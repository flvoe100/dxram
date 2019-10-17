package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class VertexLoadingConsumer implements Runnable {
    ArrayBlockingQueue<List<Long>> verticesIDPackages;
    ChunkLocalService m_chunkLocalService;
    ChunkService m_chunkService;

    public VertexLoadingConsumer(ArrayBlockingQueue<List<Long>> verticesIDPackages, ChunkLocalService m_chunkLocalService, ChunkService m_chunkService) {
        this.verticesIDPackages = verticesIDPackages;
        this.m_chunkLocalService = m_chunkLocalService;
        this.m_chunkService = m_chunkService;
    }

    @Override
    public void run() {
        try {
            List<Long> vids = verticesIDPackages.take();
            long[] cids = new long[vids.size()];
            SimpleVertex vertex = new SimpleVertex();
            for (int i = 0; i < vids.size(); i++) {
                cids[i] = vids.get(i);
            }
            m_chunkLocalService.createLocal().create(cids, cids.length, vertex.sizeofObject(), true, false);
            for (int i = 0; i < cids.length; i++) {
                vertex = new SimpleVertex(cids[i], vids.get(i));
                m_chunkService.put().put(vertex);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
