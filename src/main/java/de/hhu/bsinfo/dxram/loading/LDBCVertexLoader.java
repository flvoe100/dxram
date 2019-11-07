package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.ChunkID;
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
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

public class LDBCVertexLoader extends FileLoader {

    BlockingDeque<Long> buffer;
    VertexLoadingConsumer consumer;
    CountDownLatch m_countDownLatch;

    public LDBCVertexLoader() {
    }

    public LDBCVertexLoader(int numberOfVertices, ChunkLocalService p_chunkLocalService, ChunkService p_chunkService, short p_nodeID) {
        super(p_chunkLocalService, p_chunkService, p_nodeID);
        this.m_chunkLocalService = p_chunkLocalService;
        this.m_chunkService = p_chunkService;
        buffer = new LinkedBlockingDeque<>(10_000_000);
        this.m_countDownLatch = new CountDownLatch(numberOfVertices);
        consumer = new VertexLoadingConsumer(buffer, m_countDownLatch, m_chunkLocalService, m_chunkService);
    }

    @Override
    public VerticesTaskResponse readVerticesFile(Path p_filePath, Graph p_graph) {
        System.out.println("READING VERTICES");
        long min = Integer.MAX_VALUE;
        long max = Integer.MIN_VALUE;
        Thread t1 = new Thread(consumer);
        t1.start();
        try (final BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new BufferedInputStream(
                                Files.newInputStream(p_filePath, StandardOpenOption.READ),
                                1000000),
                        StandardCharsets.US_ASCII))) {
            String line = null;
            long vid = -1;
            int i = 0;
            while ((line = br.readLine()) != null) {
                vid = Long.parseLong(line.split("\\s")[0]);
                if (min == Integer.MAX_VALUE) {
                    min = vid;
                }
                buffer.add(vid);
                i++;
            }
            max = vid;
            System.out.println(String.format("Read %d vertices", i));
            t1.join();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return new VerticesTaskResponse(p_graph.getMasterNodeID(), m_nodeID, min, max);
    }

    @Override
    public void readFile(Path p_filePath, Graph p_graph) {

    }

}
