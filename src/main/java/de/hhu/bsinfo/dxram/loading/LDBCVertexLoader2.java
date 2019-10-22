package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LDBCVertexLoader2 extends FileLoader {

    ArrayBlockingQueue<List<Long>> buffer;
    ExecutorService executor;
    VertexLoadingConsumer consumer;

    private final static int VERTEX_PACKAGE_SIZE = 10_000;

    public LDBCVertexLoader2(ChunkLocalService p_chunkLocalService, ChunkService p_chunkService, short p_nodeID) {
        super(p_chunkLocalService, p_chunkService, p_nodeID);
        this.m_chunkLocalService = p_chunkLocalService;
        this.m_chunkService = p_chunkService;
        executor = Executors.newFixedThreadPool(3);
        buffer = new ArrayBlockingQueue<>(40);
        consumer = new VertexLoadingConsumer(buffer, m_chunkLocalService, m_chunkService);
    }

    @Override
    public void readFile(Path p_filePath, Graph p_graph) {

    }

    @Override
    public VerticesTaskResponse readVerticesFile(Path p_filePath, short p_masterNodeId) {
        long min = Integer.MAX_VALUE;
        long max = Integer.MIN_VALUE;
        try (final BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new BufferedInputStream(
                                Files.newInputStream(p_filePath, StandardOpenOption.READ),
                                1000000),
                        StandardCharsets.US_ASCII))) {
            String line = null;
            int cntVertices = 0;
            long vid = -1;

            System.gc();
            ArrayList<Long> vids = new ArrayList<>(VERTEX_PACKAGE_SIZE);
            while ((line = br.readLine()) != null) {
                vid = Long.parseLong(line.split("\\s")[0]);
                if (min == Integer.MAX_VALUE) {
                    min = vid;
                }
                vids.add(vid);
                cntVertices++;
                if (cntVertices == VERTEX_PACKAGE_SIZE) {
                    cntVertices = 0;
                    buffer.put(vids);
                    vids = new ArrayList<>(VERTEX_PACKAGE_SIZE);
                }
            }
            max = vid;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return new VerticesTaskResponse(p_masterNodeId, m_nodeID, min, max);
    }

    @Override
    public void readFile(Path p_filePath, GraphLoadingMetaData p_metaData) {

    }
}
