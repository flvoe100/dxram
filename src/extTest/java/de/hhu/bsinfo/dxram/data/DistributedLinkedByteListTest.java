package de.hhu.bsinfo.dxram.data;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.loading.DistributedLinkedByteList;
import de.hhu.bsinfo.dxram.loading.SimpleEdge;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.util.NetworkHelper;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class DistributedLinkedByteListTest {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DistributedLinkedByteListTest.class.getSimpleName());

    private static final long PARK_TIME = TimeUnit.SECONDS.toMillis(1);
    private static final String NAMESERVICE_ID = "MYLST";

    @TestInstance(runOnNodeIdx = 1)
    public void testDistributedWrite(final DXRAM dxram) {

        BootService bootService = dxram.getService(BootService.class);
        ChunkLocalService chunkLocalService = dxram.getService(ChunkLocalService.class);
        ChunkService chunkService = dxram.getService(ChunkService.class);
        NameserviceService nameserviceService = dxram.getService(NameserviceService.class);
        short ownID = bootService.getNodeID();
        short peer = NetworkHelper.findPeer(bootService);
        assertNotEquals(NodeID.INVALID_ID, peer);

        // Create a linked list
        DistributedLinkedByteList<SimpleEdge> list = DistributedLinkedByteList.create(chunkLocalService, chunkService, SimpleEdge::new);
        nameserviceService.register(list.getMetaDataID(), NAMESERVICE_ID);
        SimpleEdge[] edges = new SimpleEdge[2];
        for (int i = 0; i < 2; i++) {
            int sink = i * 10;
            edges[i] = new SimpleEdge(sink);
        }
        list.add(peer, edges);

        edges = new SimpleEdge[498];
        for (int i = 0; i < 498; i++) {
            int sink = i * 10 + 20;
            edges[i] = new SimpleEdge(sink);
        }
        list.add(peer, edges);
/*
        edges = new SimpleEdge[15998];
        for (int i = 0; i < 15998; i++) {
            int sink = i * 10 + 40;
            edges[i] = new SimpleEdge(sink);
        }
        list.add(peer, edges);
*/
        List<SimpleEdge> ret = list.getAll();
        System.out.println("List size: " +ret.size());
        for (int i = 0; i < ret.size(); i++) {
            SimpleEdge e = ret.get(i);
            assertEquals(e.getDestID(true), i * 10);
        }


    }

    @TestInstance(runOnNodeIdx = 2)
    public void testDistributedRead(final DXRAM dxram) {
        DistributedLinkedByteList<SimpleEdge> list = null;
        // Wait until first node created the linked list
        ChunkService chunkService = dxram.getService(ChunkService.class);
        NameserviceService nameserviceService = dxram.getService(NameserviceService.class);
        while (list == null) {
            try {
                long metaDataID = nameserviceService.getChunkID(NAMESERVICE_ID, 4000);
                list = DistributedLinkedByteList.get(metaDataID, chunkService, SimpleEdge::new);
            } catch (ElementNotFoundException ignored) {
            }
        }
        System.out.println("getting List");
        // Wait until first node added two elements to the linked list


        while (list.size() != 500) {
            LockSupport.parkNanos(PARK_TIME);
            System.out.println(list.size());
        }
        List<SimpleEdge> ret = list.getAll();
        System.out.println(ret.size());
        for (int i = 0; i < ret.size(); i++) {
            SimpleEdge e = ret.get(i);
            assertEquals(e.getDestID(true), i * 10);
        }


    }
}
