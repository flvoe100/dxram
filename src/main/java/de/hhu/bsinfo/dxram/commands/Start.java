package de.hhu.bsinfo.dxram.commands;

import de.hhu.bsinfo.dxnet.NetworkDeviceType;
import picocli.CommandLine;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponentConfig;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderException;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJVMArgs;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJsonFile2;
import de.hhu.bsinfo.dxram.ms.ComputeRole;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeServiceConfig;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponentConfig;
import de.hhu.bsinfo.dxram.util.NetworkUtil;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

@CommandLine.Command(name = "start", description = "Starts a new DXRAM instance.%n", showDefaultValues = true, separator = " ")
public class Start implements Runnable {

    @CommandLine.Option(
            names = "--bootstrap",
            description = "Runs this instance in bootstrapping mode.")
    private boolean m_isBootstrap = false;

    @CommandLine.Option(
            names = "--superpeer",
            description = "Runs this instance as an superpeer.")
    private boolean m_isSuperpeer = false;

    @CommandLine.Option(
            names = "--join",
            description = "The bootstrapper's connection information.",
            paramLabel = "<JOIN_ADDR>")
    private String m_bootstrapAddress = "127.0.0.1:2181";

    @CommandLine.Option(
            names = "--bind",
            description = "The local IP-address to bind to.",
            paramLabel = "<BIND_ADDR>")
    private InetSocketAddress m_bindAddress = new InetSocketAddress(NetworkUtil.getSiteLocalAddress(), 22222);

    @CommandLine.Option(
            names = "--local",
            description = "Runs this instance using the loopback interface.")
    private boolean m_isLoopback = false;

    @CommandLine.Option(
            names = "--netdev",
            description = "The network device type to be used (ethernet/infiniband).",
            paramLabel = "<NET_DEV>")
    private String m_networkDevice = NetworkDeviceType.ETHERNET.toString();

    @CommandLine.Option(
            names = "--level",
            description = "The log level to use.",
            paramLabel = "<LOG_LEVEL>")
    private String m_logLevel = "INFO";

    @CommandLine.Option(
            names = { "--storage", "--kvsize", "--kv" },
            description = "Amount of main memory to use for the key value store in MB.",
            paramLabel = "<KV_SIZE>")
    private int m_storage = 128;

    @CommandLine.Option(
            names = "--handler",
            description = "Number of threads to spawn for handling incoming and assembled network messages.",
            paramLabel = "<MESSAGE_HANDLER>")
    private int m_handler = 2;

    @CommandLine.Option(
            names = { "--master-slave-role", "--msrole" },
            description = "Compute role to assign to the current instance (${COMPLETION-CANDIDATES}).",
            paramLabel = "<COMPUTE_ROLE>")
    private ComputeRole m_msrole = ComputeRole.NONE;

    @CommandLine.Option(
            names = { "--compute-group", "--cg" },
            description = "Compute group id to assign to the current instance.",
            paramLabel = "<COMPUTE_GROUP>")
    private short m_computeGroup = 0;

    @CommandLine.Option(
            names = { "--orb-size" },
            description = "Outgoing ring buffer size for DXNet.",
            paramLabel = "<ORB_SIZE>")
    private StorageUnit orbSize = new StorageUnit(2, StorageUnit.MB);

    @CommandLine.Option(
            names = { "--ib-incoming-size" },
            description = "Incoming buffer size for ibdxnet.",
            paramLabel = "<IB_INCOMING_SIZE>")
    private StorageUnit ibIncomingSize = new StorageUnit(32, StorageUnit.KB);
    
    @Override
    public void run() {

        Configurator.setLevel("de.hhu.bsinfo", Level.toLevel(m_logLevel, Level.INFO));

        DXRAM.printBanner();

        if (m_isBootstrap) {
            m_isSuperpeer = true;
        }

        if (m_isLoopback) {
            m_bindAddress = new InetSocketAddress("127.0.0.1", 22222);
        }

        DXRAM dxram = new DXRAM();

        DXRAMConfig config = overrideConfig(dxram.createDefaultConfigInstance());

        boolean success = dxram.initialize(config, true);
        if (!success) {
            System.out.println("Initializing DXRAM failed.");
            System.exit(-1);
        }

        dxram.run();
        System.exit(0);
    }

    private DXRAMConfig overrideConfig(final DXRAMConfig p_config) {

        DXRAMConfigBuilderJsonFile2 configBuilderFile = new DXRAMConfigBuilderJsonFile2();
        DXRAMConfigBuilderJVMArgs configBuilderJvmArgs = new DXRAMConfigBuilderJVMArgs();

        DXRAMConfig overridenConfig = null;

        // JVM args override any default and/or config values loaded from file
        try {
            overridenConfig = configBuilderJvmArgs.build(configBuilderFile.build(p_config));
        } catch (final DXRAMConfigBuilderException e) {
            System.out.println("Bootstrapping configuration failed: " + e.getMessage());
            System.exit(-1);
        }

        // Set specified node role
        overridenConfig.getEngineConfig().setRole(m_isSuperpeer ? NodeRole.SUPERPEER_STR : NodeRole.PEER_STR);
        overridenConfig.getEngineConfig().setAddress(new IPV4Unit(m_bindAddress.getHostString(), m_bindAddress.getPort()));

        // Set bootstrap flag
        ZookeeperBootComponentConfig bootConfig = overridenConfig.getComponentConfig(ZookeeperBootComponent.class);
        bootConfig.setBootstrap(m_isBootstrap);

        // Set bootstrap address
        String[] connection = m_bootstrapAddress.split(":");
        bootConfig.setConnection(new IPV4Unit(connection[0], Integer.parseInt(connection[1])));

        // Set key-value storage size
        ChunkComponentConfig chunkConfig = overridenConfig.getComponentConfig(ChunkComponent.class);
        chunkConfig.setKeyValueStoreSize(new StorageUnit(m_storage, StorageUnit.MB));

        // Set Number of threads to spawn for handling incoming and assembled network messages
        NetworkComponentConfig netConfig = overridenConfig.getComponentConfig(NetworkComponent.class);
        netConfig.getCoreConfig().setNumMessageHandlerThreads(m_handler);

        // Set the network device type
        netConfig.getCoreConfig().setDevice(m_networkDevice);

        // Set ORB size
        if(m_networkDevice.equals(NetworkDeviceType.ETHERNET_STR)) {
            netConfig.getNioConfig().setOutgoingRingBufferSize(orbSize);
            netConfig.getNioConfig().setFlowControlWindow(new StorageUnit(orbSize.getBytes() / 2, StorageUnit.BYTE));
        } else if(m_networkDevice.equals(NetworkDeviceType.INFINIBAND_STR)){
            netConfig.getIbConfig().setOutgoingRingBufferSize(orbSize);
            netConfig.getIbConfig().setFlowControlWindow(new StorageUnit(orbSize.getBytes() / 2, StorageUnit.BYTE));
            netConfig.getIbConfig().setIncomingBufferSize(ibIncomingSize);
        }

        // Set the compute role and compute group id to assign to the current instance (master, slave or none)
        MasterSlaveComputeServiceConfig msConfig = overridenConfig.getServiceConfig(MasterSlaveComputeService.class);
        msConfig.setRole(m_msrole.toString());            
        msConfig.setComputeGroupId(m_computeGroup);

        return overridenConfig;
    }
}
