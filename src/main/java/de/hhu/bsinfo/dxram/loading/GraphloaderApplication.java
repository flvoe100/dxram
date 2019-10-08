package de.hhu.bsinfo.dxram.loading;


import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * "Hello world" example DXRAM application.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public class GraphloaderApplication extends Application {

    private final Logger LOGGER = LogManager.getFormatterLogger(GraphloaderApplication.class);

    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "GraphloaderApplication";
    }

    @Override
    public void main(final String[] p_args) {
        ChunkLocalService chunkLocalService = this.getService(ChunkLocalService.class);
        ChunkService chunkService = this.getService(ChunkService.class);
        BootService bootService = this.getService(BootService.class);
        NameserviceService nameService = this.getService(NameserviceService.class);
        SynchronizationService syncService = this.getService(SynchronizationService.class);
        NetworkService networkService = this.getService(NetworkService.class);
        int i = 0;
        String filesDirectoryPath  = p_args[i++];
        String datasetFormat = p_args[i++];



        //boolean saveVertix = Boolean.parseBoolean(p_args[3]);

        short currentNodeID = bootService.getNodeID();
        short coordinatorID = bootService.getOnlinePeerNodeIDs().get(0);
        boolean isCoordinator = coordinatorID == currentNodeID;

        this.signalShutdown();
        System.exit(0);
    }

    @Override
    public void signalShutdown() {
        // Interrupt any flow of your application and make sure it shuts down.
        // Do not block here or wait for something to shut down. Shutting down of your application
        // must be execute asynchronously
    }
}
