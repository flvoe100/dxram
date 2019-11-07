package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

public class LDBCEdgeLoader extends FileLoader {

    private BlockingDeque<Pair<Long>> m_buffer;
    private EdgesLoadingConsumer m_consumer;
    private CountDownLatch m_countDownLatch;
    private GraphLoadingMetaData m_metaData;

    private final Logger LOGGER = LogManager.getFormatterLogger(LDBCEdgeLoader.class);

    public LDBCEdgeLoader() {
    }

    public LDBCEdgeLoader(int numberOfEdges, GraphLoadingMetaData p_metaData, ChunkLocalService p_chunkLocalService, ChunkService p_chunkService, short p_nodeID) {
        super(p_chunkLocalService, p_chunkService, p_nodeID);
        this.m_metaData = p_metaData;
        this.m_chunkLocalService = p_chunkLocalService;
        this.m_chunkService = p_chunkService;
        m_buffer = new LinkedBlockingDeque<>(10_000_000);
        this.m_countDownLatch = new CountDownLatch(numberOfEdges);
        m_consumer = new EdgesLoadingConsumer(m_buffer, m_countDownLatch, m_chunkLocalService, m_chunkService);
    }


    @Override
    public void readFile(Path p_filePath, Graph p_graph) {
        LOGGER.info("Loading edges!");

        Thread t1 = new Thread(m_consumer);
        t1.start();

        try (final BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new BufferedInputStream(
                                Files.newInputStream(p_filePath, StandardOpenOption.READ),
                                1000000),
                        StandardCharsets.US_ASCII))) {
            String line = null;
            long from = -1;
            long to = -1;
            Pair<Long> pair;
            int i = 0;
            while ((line = br.readLine()) != null) {
                String[] split = line.split("\\s");
                from = Long.parseLong(split[0]);
                to = Long.parseLong(split[1]);
                //we only load edges where the source vertex is local
                short sinkNodeID = -1;
                for (int j = 0; j < m_metaData.m_peers.length; j++) {
                    short slaveID = m_metaData.m_peers[j];
                    if (Util.isInInterval(m_metaData.getStartIntervalOfNode(slaveID), m_metaData.getEndIntervalOfNode(slaveID), to)) {
                        sinkNodeID = slaveID;
                    }
                }
                if (sinkNodeID == -1) {
                    LOGGER.error("Belonging vertex id of sink of edge [%d, %d]", from, to);
                }
                pair = new Pair<>(ChunkID.getChunkID(m_nodeID, from), ChunkID.getChunkID(sinkNodeID, to));
                m_buffer.put(pair);
                i++;
            }
            System.out.println(String.format("Read %d edges", i));
            t1.join();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public VerticesTaskResponse readVerticesFile(Path p_filePath, Graph p_graph) {
        return null;
    }
}
