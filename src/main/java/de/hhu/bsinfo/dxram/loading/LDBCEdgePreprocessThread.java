package de.hhu.bsinfo.dxram.loading;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class LDBCEdgePreprocessThread extends Thread {
    private String path;
    private ArrayBlockingQueue<List<Edge>> queue;
    private Partition m_partition;
    private List<Partition> m_partitions;
    private PartitionPartition m_pp;
    private short m_nodeID;
    private boolean m_isDirected;
    private static final int BATCH_SIZE = 100_000;

    public LDBCEdgePreprocessThread(String path, ArrayBlockingQueue<List<Edge>> queue, Partition m_partition, List<Partition> p_partitions, PartitionPartition m_pp, boolean p_isDirected, short p_nodeID) {
        this.path = path;
        this.queue = queue;
        this.m_partition = m_partition;
        this.m_pp = m_pp;
        this.m_nodeID = p_nodeID;
        this.m_partitions = p_partitions;
        this.m_isDirected = p_isDirected;
    }

    @Override
    public void run() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            ArrayList<Edge> edgeBatch = new ArrayList<>(BATCH_SIZE);
            String line;

            long start = System.nanoTime();
            long startSkip = System.nanoTime();
            br.skip(m_pp.getFromByteOffset());
            long endSkip = System.nanoTime();

            System.out.println("Duration of skip: " + (endSkip - startSkip));
            while ((line = br.readLine()) != null) {
                if (line.equals(m_pp.getTo())) {
                    queue.put(edgeBatch);
                    break;
                }

                String[] split = line.split("\\s");
                long from = Long.parseLong(split[0]);
                long to = Long.parseLong(split[1]);
                if (m_partition.isBetween(from)) {

                    edgeBatch.add(new Edge(from, to));

                }
                if (!m_isDirected && m_partition.isBetween(to)) {
                    edgeBatch.add(new Edge(to, from));

                }
                if (edgeBatch.size() >= BATCH_SIZE) {
                    queue.put(edgeBatch);
                    edgeBatch = new ArrayList<>(BATCH_SIZE);
                }
            }
            queue.put(edgeBatch);
            long duration = System.nanoTime() - start;
            System.out.println("duration = " + duration);


        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
