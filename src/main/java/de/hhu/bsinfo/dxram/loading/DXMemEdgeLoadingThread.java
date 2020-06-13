package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static de.hhu.bsinfo.dxram.loading.Partition.getNodeIDOfVertix;

public class DXMemEdgeLoadingThread extends Thread {
    private ArrayBlockingQueue<List<Edge>> m_edgeQueue;
    private HashMap<Long, Vertex> m_idToVertexMap;
    private List<Partition> m_partitions;
    private ChunkService m_chunkService;
    private ChunkLocalService m_chunkLocalService;
    private short m_nodeID;

    private static final Logger LOGGER = LogManager.getFormatterLogger(DXMemEdgeLoadingThread.class.getSimpleName());

    public DXMemEdgeLoadingThread(ArrayBlockingQueue<List<Edge>> p_edgeQueue, List<Partition> p_partitions, HashMap<Long, Vertex> vertexHashMap, ChunkService p_chunkService, ChunkLocalService p_chunkLocalService, short p_nodeID) {
        this.m_edgeQueue = p_edgeQueue;
        this.m_partitions = p_partitions;
        this.m_chunkService = p_chunkService;
        this.m_chunkLocalService = p_chunkLocalService;
        this.m_nodeID = p_nodeID;
        this.m_idToVertexMap = vertexHashMap;
    }

    @Override
    public void run() {
        long start = System.nanoTime();
        HashMap<Long, List<Long>> map = new HashMap<>();
        while (true) {
            try {
                List<Edge> edgeBatch = m_edgeQueue.poll(5, TimeUnit.SECONDS);
                if (edgeBatch == null) {
                    break;
                }
                for (Edge e : edgeBatch) {
                    if (map.containsKey(e.getSourceID())) {
                        List<Long> neighbours = map.get(e.getSourceID());
                        neighbours.add(e.getDestID());
                        map.put(e.getSourceID(), neighbours);
                    } else {
                        ArrayList<Long> neighbours = new ArrayList<>();
                        neighbours.add(e.getDestID());
                        map.put(e.getSourceID(), neighbours);
                    }
                }
                loadEdgesIntoSystem(map);
                map = new HashMap<>();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long duration = System.nanoTime() - start;
        System.out.println("Duration to load graph into system = " + duration);
    }


    public void loadEdgesIntoSystem(HashMap<Long, List<Long>> p_map) {
        //now edges
        Iterator<Map.Entry<Long, List<Long>>> it = p_map.entrySet().iterator();
        SimpleEdge[] neighbours;
        while (it.hasNext()) {
            Map.Entry<Long, List<Long>> entry = it.next();
            DistributedLinkedByteList<SimpleEdge> linkedNeighbourList = null;
            Vertex v = m_idToVertexMap.get(entry.getKey());

            if (v.getNeighbourLinkedList() == null) {
                linkedNeighbourList = DistributedLinkedByteList.create(m_chunkLocalService, m_chunkService, SimpleEdge::new);
                v.setNeighbourLinkedList(linkedNeighbourList);
            } else {
                linkedNeighbourList = v.getNeighbourLinkedList();
            }

            if (linkedNeighbourList == null) {
                System.err.println(String.format("ERROR: No linked neighbour list found for vertice: %d", entry.getKey()));
                return;
            }
            List<Long> neighbourList = entry.getValue();
            neighbours = new SimpleEdge[neighbourList.size()];
            for (int i = 0; i < neighbourList.size(); i++) {
                neighbours[i] = new SimpleEdge(getNodeIDOfVertix(m_partitions, neighbourList.get(i)));
            }
            linkedNeighbourList.add(m_nodeID, neighbours);
        }
    }
}
