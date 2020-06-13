package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.ComputeRole;
import de.hhu.bsinfo.dxram.util.NodeRole;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER, networkRequestResponseTimeoutMs = 5000),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, masterSlaveComputeRole = ComputeRole.MASTER,
                        networkRequestResponseTimeoutMs = 5000),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER, masterSlaveComputeRole = ComputeRole.SLAVE, networkRequestResponseTimeoutMs = 5000, keyValueStorageSizeMB = 4000)

        })
public class LoadingTest {

    public static final int PACKAGE_SIZE = 100;

    @TestInstance(runOnNodeIdx = 1)
    public void loadingTest(final DXRAM p_instance) throws IOException {
        Configurator.setRootLevel(Level.DEBUG);
        BootService bootService = p_instance.getService(BootService.class);
        short nodeID = bootService.getNodeID();
        ChunkLocalService chunkLocalService = p_instance.getService(ChunkLocalService.class);
        ChunkService chunkService = p_instance.getService(ChunkService.class);

        DistributedLinkedByteList<SimpleEdge> list = DistributedLinkedByteList.create(chunkLocalService, chunkService, SimpleEdge::new);
        long metadataID = list.getMetaDataID();
        SimpleEdge[] edges = new SimpleEdge[16000];
        for (int i = 0; i < edges.length; i++) {
            edges[i] = new SimpleEdge(i + 1);
        }
        list.add(nodeID, edges);
        List<SimpleEdge> edgeList = list.getAll();
        for (int i = 0; i < edges.length; i++) {
            SimpleEdge e = edgeList.get(i);
            assert e.getDestID(true) == i + 1;
        }
        edgeList = null;
        list = null;
        list = DistributedLinkedByteList.get(metadataID, chunkService, SimpleEdge::new);

        edgeList = list.getAll();
        for (int i = 0; i < edges.length; i++) {
            SimpleEdge e = edgeList.get(i);
            assert e.getDestID(true) == i + 1;
        }


/*
        String filesDirectoryPath = "/home/vlz/bsinfo/datasets/dota-league/";
        String datasetName = "dota-league";
        int lastLineSize = 16;
        LDBCFormat format = new LDBCFormat(filesDirectoryPath, datasetName);

        DxGraph graph = new DxGraph(p_instance, format, lastLineSize);

        graph.loadGraph();
 */

    }

    /*

    @TestInstance(runOnNodeIdx = 1)
    public void loadingVerticesTest(final DXRAM p_instance) throws IOException {
        Configurator.setRootLevel(Level.DEBUG);
        BootService bootService = p_instance.getService(BootService.class);
        short nodeID = bootService.getNodeID();
        ChunkLocalService chunkLocalService = p_instance.getService(ChunkLocalService.class);

        ChunkService chunkService = p_instance.getService(ChunkService.class);

        BufferedReader br = new BufferedReader(new FileReader("/home/vlz/bsinfo/datasets/datagen-8_9-fb.v"));
        String line;
        Vertex v = new Vertex();
        long from = 0;
        long to = 2199033544756L;
        ArrayList<Long> vs = new ArrayList<>();
        long[] p_cids;
        while ((line = br.readLine()) != null) {
            long vid = Long.parseLong(line.split("\\s")[0]);
            if(vid >= from && vid <= to) {
                vs.add(vid);
            } else {
                continue;
            }


            if (vs.size() == PACKAGE_SIZE) {
                p_cids = new long[PACKAGE_SIZE];
                for (int j = 0; j < PACKAGE_SIZE; j++) {
                    p_cids[j] = vs.get(j);
                }
                int created = chunkLocalService.createLocal().create(p_cids, PACKAGE_SIZE, v.sizeofObject(), true, false);
                assert created == PACKAGE_SIZE;
                vs = new ArrayList<>();
            }
        }

        if (vs.size() == PACKAGE_SIZE) {
            p_cids = new long[PACKAGE_SIZE];
            for (int j = 0; j < PACKAGE_SIZE; j++) {
                p_cids[j] = vs.get(j);
            }
            int created = chunkLocalService.createLocal().create(p_cids, PACKAGE_SIZE, v.sizeofObject(), true, false);
            assert created == PACKAGE_SIZE;
        }
    }
     */

}
