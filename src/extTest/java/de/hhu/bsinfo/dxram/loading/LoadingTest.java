package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.ComputeRole;
import de.hhu.bsinfo.dxram.util.NodeRole;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.FileReader;
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

    @TestInstance(runOnNodeIdx = 1)
    public void loadingTest(final DXRAM p_instance) throws IOException {
        Configurator.setRootLevel(Level.INFO);

        BootService bootService = p_instance.getService(BootService.class);
        short nodeID = 0;

        List<Short> nodes = bootService.getOnlinePeerNodeIDs();
        for (Short id : nodes
        ) {
            if (bootService.getNodeID() != id) {
                nodeID = id;
                break;
            }
        }

        LDBCFormat format = new LDBCFormat("/home/vlz/bsinfo/datasets/dota-league", "dota-league");
        DxGraph graph = new DxGraph(p_instance, format, true);

        graph.loadGraph();
        ChunkService m_chunkService = p_instance.getService(ChunkService.class);
        BufferedReader br = new BufferedReader(new FileReader("/home/vlz/bsinfo/datasets/dota-league/dota-league.v"));
        String line;
        /*
        while ((line = br.readLine()) != null) {
            SimpleVertex v = new SimpleVertex();
            long vid = Long.parseLong(line.split("\\s")[0]);
            v.setID(ChunkID.getChunkID((short) -1483, vid));
            m_chunkService.get().get(v);
            if (ChunkID.getLocalID(v.getID()) != v.getExtID()) {
                System.out.println("ChunkID.getLocalID(v.getID()) = " + ChunkID.getLocalID(v.getID()));
                System.out.println("v.getExtID() = " + v.getExtID());
            }
        }

         */
        SimpleEdge e = new SimpleEdge();
        e.setID(ChunkID.getChunkID((short) -1483, 3));
        m_chunkService.get().get(e);
        System.out.println("Edge = " + e.getID());


    }

}
