package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import org.junit.runner.RunWith;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class LoadingTest {

    @TestInstance(runOnNodeIdx = 2)
    public void loadingTest(final DXRAM p_instance) {
        BootService bootService = p_instance.getService(BootService.class);

        LDBCFormat format = new LDBCFormat("/home/vlz/bsinfo/datasets/dota-league/", "dota-league");
        GraphLoadingMetaData metaData = new GraphLoadingMetaData(bootService.getOnlinePeerNodeIDs());
        DxGraph graph = new DxGraph("/home/vlz/bsinfo/datasets/dota-league/", format, metaData, true);

        graph.loadGraph();
    }

}
