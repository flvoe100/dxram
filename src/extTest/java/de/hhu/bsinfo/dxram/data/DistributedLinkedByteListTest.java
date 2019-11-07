package de.hhu.bsinfo.dxram.data;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.loading.DistributedLinkedByteList;
import de.hhu.bsinfo.dxram.loading.SimpleEdge;
import de.hhu.bsinfo.dxram.util.NetworkHelper;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Random;
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
        short ownID = bootService.getNodeID();
        short peer = NetworkHelper.findPeer(bootService);
        assertNotEquals(NodeID.INVALID_ID, peer);

        // Create a linked list
        DistributedLinkedByteList<SimpleEdge> list = DistributedLinkedByteList.create(NAMESERVICE_ID, dxram, SimpleEdge::new);
        SimpleEdge[] edges = new SimpleEdge[3];
        Random rnd = new Random();
        for (int i = 0; i < 3; i++) {
            int sink = i * 10;
            System.out.println("Putting edge with sink = " + sink);
            edges[i] = new SimpleEdge(sink);
        }
        list.add(edges, peer);
        edges = new SimpleEdge[4];
        for (int i = 0; i < 4; i++) {
            int sink = (i + 3) * 10;
            System.out.println("Putting edge with sink = " + sink);
            edges[i] = new SimpleEdge(sink);
        }
        list.add(edges, peer);
        /*
        // Add (append) an element storing the data on a remote peer
        list.add("Hello", peer);

        // Add (append) an element storing the data on this peer
        list.add("World", bootService.getNodeID());

        // Add an element at a specific index storing the data on a remote peer
        list.add(1, "Distributed", peer);

         */
    }

    @TestInstance(runOnNodeIdx = 2)
    public void testDistributedRead(final DXRAM dxram) {
        DistributedLinkedByteList<SimpleEdge> list = null;

        // Wait until first node created the linked list
        while (list == null) {
            try {
                list = DistributedLinkedByteList.get(NAMESERVICE_ID, dxram, SimpleEdge::new);
            } catch (ElementNotFoundException ignored) {
            }
        }

        // Wait until first node added two elements to the linked list
        while (list.size() != 7) {
            LockSupport.parkNanos(PARK_TIME);
        }
        List<SimpleEdge> ret = list.getAll();
        assertEquals(ret.size(), 7);
        for (int i = 0; i < ret.size(); i++) {
            SimpleEdge e = ret.get(i);
            System.out.println(e.getSinkID());
            LOGGER.info(String.format("Edge with sink = %d", e.getSinkID()));
            assertEquals(e.getSinkID(), i * 10);
        }


    }
}
