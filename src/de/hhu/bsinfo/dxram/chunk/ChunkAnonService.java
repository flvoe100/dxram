/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.chunk;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.GetAnonRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetAnonResponse;
import de.hhu.bsinfo.dxram.chunk.messages.PutAnonRequest;
import de.hhu.bsinfo.dxram.chunk.messages.PutAnonResponse;
import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.ChunkState;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMRuntimeException;
import de.hhu.bsinfo.dxram.engine.InvalidNodeRoleException;
import de.hhu.bsinfo.dxram.lock.AbstractLockComponent;
import de.hhu.bsinfo.dxram.log.messages.LogAnonMessage;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Special chunk service to work with anonymous chunks i.e. chunks with unknown size when getting them
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.03.2017
 */
public class ChunkAnonService extends AbstractDXRAMService implements MessageReceiver {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkService.class.getSimpleName());

    // statistics recording
    private static final StatisticsOperation SOP_GET_ANON = StatisticsRecorderManager.getOperation(ChunkService.class, "GetAnon");
    private static final StatisticsOperation SOP_INCOMING_GET_ANON = StatisticsRecorderManager.getOperation(ChunkService.class, "IncomingGetAnon");
    private static final StatisticsOperation SOP_PUT_ANON = StatisticsRecorderManager.getOperation(ChunkService.class, "PutAnon");
    private static final StatisticsOperation SOP_INCOMING_PUT_ANON = StatisticsRecorderManager.getOperation(ChunkService.class, "IncomingPutAnon");

    // dependent components
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private MemoryManagerComponent m_memoryManager;
    private NetworkComponent m_network;
    private LookupComponent m_lookup;
    private AbstractLockComponent m_lock;

    /**
     * Constructor
     */
    public ChunkAnonService() {
        super("chunkanon");
    }

    /**
     * Get/Read the data stored in the backend storage for chunks of unknown size. Use this if the payload size is
     * unknown, only!
     *
     * @param p_chunkIDs
     *     Array with ChunkIDs.
     * @return Array of anonymous chunks matching the order of the provided chunk ID array.
     */
    public ChunkAnon[] get(final long... p_chunkIDs) {
        ChunkAnon[] ret;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // #if LOGGER == TRACE
        LOGGER.trace("get[chunkIDs(%d) ...]", p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_GET_ANON.enter(p_chunkIDs.length);
        // #endif /* STATISTICS */

        // sort by local and remote data first
        Map<Short, ArrayList<Integer>> remoteChunkIDsByPeers = new TreeMap<>();

        ret = new ChunkAnon[p_chunkIDs.length];
        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < p_chunkIDs.length; i++) {
                // try to get locally, will check first if it exists and
                // returns false if it doesn't exist
                byte[] data = m_memoryManager.get(p_chunkIDs[i]);
                if (data != null) {
                    ret[i] = new ChunkAnon(p_chunkIDs[i], data);
                    ret[i].setState(ChunkState.OK);
                } else {
                    // remote or migrated, figure out location and sort by peers
                    LookupRange lookupRange;

                    lookupRange = m_lookup.getLookupRange(p_chunkIDs[i]);
                    if (lookupRange != null) {
                        short peer = lookupRange.getPrimaryPeer();

                        ArrayList<Integer> remoteChunkIDsOfPeer = remoteChunkIDsByPeers.computeIfAbsent(peer, a -> new ArrayList<>());
                        // Add the index in ChunkID array not the ChunkID itself
                        remoteChunkIDsOfPeer.add(i);
                    }
                }
            }
        } finally {
            m_memoryManager.unlockAccess();
        }

        // go for remote ones by each peer
        for (final Map.Entry<Short, ArrayList<Integer>> peerWithChunks : remoteChunkIDsByPeers.entrySet()) {
            short peer = peerWithChunks.getKey();
            ArrayList<Integer> remoteChunkIDIndexes = peerWithChunks.getValue();

            if (peer == m_boot.getNodeID()) {
                // local get, migrated data to current node
                try {
                    m_memoryManager.lockAccess();
                    for (final int index : remoteChunkIDIndexes) {
                        ret[index] = new ChunkAnon(p_chunkIDs[index], m_memoryManager.get(p_chunkIDs[index]));
                        ret[index].setState(ChunkState.OK);
                    }
                } finally {
                    m_memoryManager.unlockAccess();
                }
            } else {
                // Remote get from specified peer
                int i = 0;
                ChunkAnon[] chunks = new ChunkAnon[remoteChunkIDIndexes.size()];
                for (int index : remoteChunkIDIndexes) {
                    ret[index] = new ChunkAnon(p_chunkIDs[index]);
                    chunks[i++] = ret[index];
                }
                GetAnonRequest request = new GetAnonRequest(peer, chunks);

                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    if (m_backup.isActive()) {
                        for (ChunkAnon chunk : chunks) {
                            chunk.setState(ChunkState.DATA_TEMPORARY_UNAVAILABLE);
                        }
                    } else {
                        for (ChunkAnon chunk : chunks) {
                            chunk.setState(ChunkState.DATA_LOST);
                        }
                    }

                    // #if LOGGER >= ERROR
                    LOGGER.error("Sending chunk get request to peer 0x%X failed: %s", peer, e);
                    // #endif /* LOGGER >= ERROR */
                }

                // no need to get the response
                // request.getResponse(GetResponse.class);
            }
        }

        // #ifdef STATISTICS
        SOP_GET_ANON.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("get[chunkIDs(%d) ...] -> %d", p_chunkIDs.length, p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Special local only get version. Use this if you already delegate tasks with non local
     * chunks to the remote owning them. This speeds up access to local only chunks a lot.
     * Get/Read the data stored in the backend storage for chunks of unknown size. Use this if the payload size is
     * unknown, only!
     *
     * @param p_chunkIDs
     *     Array with ChunkIDs.
     * @return Array of ChunkAnons matching the chunk ID array with the IDs assigned. Data for chunks that do not exist have the specific state set.
     */
    public ChunkAnon[] getLocal(final long... p_chunkIDs) {
        ChunkAnon[] ret;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // #if LOGGER == TRACE
        LOGGER.trace("getLocal[chunkIDs(%d) ...]", p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_GET_ANON.enter(p_chunkIDs.length);
        // #endif /* STATISTICS */

        ret = new ChunkAnon[p_chunkIDs.length];

        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < p_chunkIDs.length; i++) {
                byte[] data = m_memoryManager.get(p_chunkIDs[i]);

                if (data == null) {
                    ret[i] = new ChunkAnon(p_chunkIDs[i]);
                    ret[i].setState(ChunkState.DOES_NOT_EXIST);
                } else {
                    ret[i] = new ChunkAnon(p_chunkIDs[i], data);
                    ret[i].setState(ChunkState.OK);
                }
            }
        } finally {
            m_memoryManager.unlockAccess();
        }

        // #ifdef STATISTICS
        SOP_GET_ANON.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("getLocal[chunkIDs(%d) ...] -> %d", p_chunkIDs.length, p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    @Override
    public void onIncomingMessage(final AbstractMessage p_message) {
        // #if LOGGER == TRACE
        LOGGER.trace("Entering incomingMessage with: p_message=%s", p_message);
        // #endif /* LOGGER == TRACE */

        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case ChunkMessages.SUBTYPE_GET_ANON_REQUEST:
                        incomingGetBufferRequest((GetAnonRequest) p_message);
                        break;
                    case ChunkMessages.SUBTYPE_PUT_BUFFER_REQUEST:
                        incomingPutBufferRequest((PutAnonRequest) p_message);
                        break;
                    default:
                        break;
                }
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting incomingMessage");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Put/Update the contents of the provided Chunk in the backend storage.
     *
     * @param p_chunks
     *     ChunkAnons to put/update. Null values are ignored.
     * @return Number of successfully updated data structures.
     */
    public int put(final ChunkAnon... p_chunks) {
        return put(ChunkLockOperation.NO_LOCK_OPERATION, p_chunks);
    }

    /**
     * Put/Update the contents of the provided Chunk in the backend storage.
     *
     * @param p_chunkUnlockOperation
     *     Unlock operation to execute right after the put operation.
     * @param p_chunks
     *     ChunkAnons to put/update. Null values or chunks with invalid IDs are ignored.
     * @return Number of successfully updated data structures.
     */
    public int put(final ChunkLockOperation p_chunkUnlockOperation, final ChunkAnon... p_chunks) {
        return put(p_chunkUnlockOperation, p_chunks, 0, p_chunks.length);
    }

    /**
     * Put/Update the contents of the provided ChunkBuffers in the backend storage.
     *
     * @param p_chunkUnlockOperation
     *     Unlock operation to execute right after the put operation.
     * @param p_chunks
     *     ChunkAnons to put/update. Null values or chunks with invalid IDs are ignored.
     * @param p_offset
     *     Start offset within the array.
     * @param p_count
     *     Number of items to put.
     * @return Number of successfully updated data structures.
     */
    public int put(final ChunkLockOperation p_chunkUnlockOperation, final ChunkAnon[] p_chunks, final int p_offset, final int p_count) {
        int chunksPut = 0;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // #if LOGGER == TRACE
        LOGGER.trace("put[unlockOp %s, dataStructures(%d) ...]", p_chunkUnlockOperation, p_chunks.length);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_PUT_ANON.enter(p_count);
        // #endif /* STATISTICS */

        Map<Short, ArrayList<ChunkAnon>> remoteChunksByPeers = new TreeMap<>();
        Map<BackupRange, ArrayList<ChunkAnon>> remoteChunksByBackupRange = new TreeMap<>();

        // sort by local/remote chunks
        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < p_count; i++) {
                // filter null values
                if (p_chunks[i + p_offset] == null) {
                    continue;
                }

                // try to put every chunk locally, returns false if it does not exist
                // and saves us an additional check
                if (m_memoryManager.put(p_chunks[i + p_offset].getID(), p_chunks[i + p_offset].getData())) {
                    chunksPut++;
                    p_chunks[i + p_offset].setState(ChunkState.OK);

                    // unlock chunk as well
                    if (p_chunkUnlockOperation != ChunkLockOperation.NO_LOCK_OPERATION) {
                        boolean writeLock = false;
                        if (p_chunkUnlockOperation == ChunkLockOperation.WRITE_LOCK) {
                            writeLock = true;
                        }
                        m_lock.unlock(p_chunks[i + p_offset].getID(), m_boot.getNodeID(), writeLock);
                    }

                    if (m_backup.isActive()) {
                        // sort by backup peers
                        BackupRange backupRange = m_backup.getBackupRange(p_chunks[i + p_offset].getID());
                        ArrayList<ChunkAnon> remoteChunksOfBackupRange = remoteChunksByBackupRange.computeIfAbsent(backupRange, a -> new ArrayList<>());
                        remoteChunksOfBackupRange.add(p_chunks[i + p_offset]);
                    }
                } else {
                    // remote or migrated, figure out location and sort by peers
                    LookupRange location = m_lookup.getLookupRange(p_chunks[i + p_offset].getID());
                    if (location != null) {
                        short peer = location.getPrimaryPeer();

                        ArrayList<ChunkAnon> remoteChunksOfPeer = remoteChunksByPeers.computeIfAbsent(peer, a -> new ArrayList<>());
                        remoteChunksOfPeer.add(p_chunks[i + p_offset]);
                    } else {
                        p_chunks[i + p_offset].setState(ChunkState.DOES_NOT_EXIST);
                    }
                }
            }
        } finally {
            m_memoryManager.unlockAccess();
        }

        // go for remote chunks
        for (Map.Entry<Short, ArrayList<ChunkAnon>> entry : remoteChunksByPeers.entrySet()) {
            short peer = entry.getKey();

            if (peer == m_boot.getNodeID()) {
                // local put, migrated data to current node
                try {
                    m_memoryManager.lockAccess();
                    for (final ChunkAnon chunk : entry.getValue()) {
                        if (m_memoryManager.put(chunk.getID(), chunk.getData())) {
                            chunksPut++;
                            chunk.setState(ChunkState.OK);
                        }
                        // else: put failed, state for chunk set by memory manager
                    }
                } finally {
                    m_memoryManager.unlockAccess();
                }
            } else {
                // Remote put
                ArrayList<ChunkAnon> chunksToPut = entry.getValue();
                PutAnonRequest request = new PutAnonRequest(peer, p_chunkUnlockOperation, chunksToPut.toArray(new ChunkAnon[chunksToPut.size()]));

                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    if (m_backup.isActive()) {
                        for (ChunkAnon chunk : chunksToPut) {
                            chunk.setState(ChunkState.DATA_TEMPORARY_UNAVAILABLE);
                        }
                    } else {
                        for (ChunkAnon chunk : chunksToPut) {
                            chunk.setState(ChunkState.DATA_LOST);
                        }
                    }

                    // TODO Kevin ???
                    // m_lookup.invalidate(dataStructure.getID());

                    continue;
                }

                PutAnonResponse response = request.getResponse(PutAnonResponse.class);

                byte[] statusCodes = response.getStatusCodes();
                // try short cut, i.e. all puts successful
                if (statusCodes.length == 1 && statusCodes[0] == ChunkState.OK.ordinal()) {
                    chunksPut += chunksToPut.size();

                    for (ChunkAnon chunk : chunksToPut) {
                        chunk.setState(ChunkState.OK);
                    }
                } else {
                    for (int i = 0; i < statusCodes.length; i++) {
                        chunksToPut.get(i).setState(ChunkState.values()[statusCodes[i]]);
                        if (statusCodes[i] == ChunkState.OK.ordinal()) {
                            chunksPut++;
                        }
                    }
                }
            }
        }

        // Send backups
        if (m_backup.isActive()) {
            BackupRange backupRange;
            short[] backupPeers;
            ChunkAnon[] chunks;
            for (Map.Entry<BackupRange, ArrayList<ChunkAnon>> entry : remoteChunksByBackupRange.entrySet()) {
                backupRange = entry.getKey();
                chunks = entry.getValue().toArray(new ChunkAnon[entry.getValue().size()]);

                backupPeers = backupRange.getBackupPeers();
                for (short backupPeer : backupPeers) {
                    if (backupPeer != NodeID.INVALID_ID) {
                        // #if LOGGER == TRACE
                        LOGGER.trace("Logging %d chunks to 0x%X", chunks.length, backupPeer);
                        // #endif /* LOGGER == TRACE */

                        try {
                            m_network.sendMessage(new LogAnonMessage(backupPeer, backupRange.getRangeID(), chunks));
                        } catch (final NetworkException ignore) {

                        }
                    }
                }
            }
        }

        // #ifdef STATISTICS
        SOP_PUT_ANON.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("put[unlockOp %s, dataStructures(%d) ...] -> %d", p_chunkUnlockOperation, p_chunks.length, chunksPut);
        // #endif /* LOGGER == TRACE */

        return chunksPut;
    }

    // -----------------------------------------------------------------------------------

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_lock = p_componentAccessor.getComponent(AbstractLockComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        registerNetworkMessages();
        registerNetworkMessageListener();

        if (p_engineEngineSettings.getRole() == NodeRole.PEER && m_backup.isActive()) {
            if (m_memoryManager.getStatus().getMaxChunkSize().getBytes() > m_backup.getLogSegmentSizeBytes()) {
                LOGGER.fatal("Backup is active and segment size (%d bytes) of log is smaller than max chunk size (%d bytes). Fix your configuration");
                throw new DXRAMRuntimeException("Backup is active and segment size of log is smaller than max chunk size. Fix your configuration");
            }
        }

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    // -----------------------------------------------------------------------------------

    /**
     * Register network messages we use in here.
     */
    private void registerNetworkMessages() {
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_ANON_REQUEST, GetAnonRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_ANON_RESPONSE, GetAnonResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_BUFFER_REQUEST, PutAnonRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_BUFFER_RESPONSE, PutAnonResponse.class);
    }

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network.register(GetAnonRequest.class, this);
        m_network.register(PutAnonRequest.class, this);
    }

    // -----------------------------------------------------------------------------------

    /**
     * Handles an incoming GetAnonRequest
     *
     * @param p_request
     *     the GetAnonRequest
     */
    private void incomingGetBufferRequest(final GetAnonRequest p_request) {
        long[] chunkIDs = p_request.getChunkIDs();
        byte[][] data = new byte[chunkIDs.length][];
        int numChunksGot = 0;

        // #ifdef STATISTICS
        SOP_INCOMING_GET_ANON.enter(p_request.getChunkIDs().length);
        // #endif /* STATISTICS */

        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < data.length; i++) {
                // also does exist check
                data[i] = m_memoryManager.get(chunkIDs[i]);

                if (data[i] != null) {
                    numChunksGot++;
                }
            }
        } finally {
            m_memoryManager.unlockAccess();
        }

        GetAnonResponse response = new GetAnonResponse(p_request, data, numChunksGot);

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending GetAnonResponse for %d chunks failed: %s", numChunksGot, e);
            // #endif /* LOGGER >= ERROR */
        }

        // #ifdef STATISTICS
        SOP_INCOMING_GET_ANON.leave();
        // #endif /* STATISTICS */
    }

    /**
     * Handles an incoming PutAnonRequest
     *
     * @param p_request
     *     the PutAnonRequest
     */
    private void incomingPutBufferRequest(final PutAnonRequest p_request) {
        long[] chunkIDs = p_request.getChunkIDs();
        byte[][] data = p_request.getChunkData();

        byte[] statusChunks = new byte[chunkIDs.length];
        boolean allSuccessful = true;

        // #ifdef STATISTICS
        SOP_INCOMING_PUT_ANON.enter(chunkIDs.length);
        // #endif /* STATISTICS */

        Map<BackupRange, ArrayList<ChunkAnon>> remoteChunksByBackupRange = new TreeMap<>();

        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < chunkIDs.length; i++) {
                if (!m_memoryManager.put(chunkIDs[i], data[i])) {
                    // does not exist (anymore)
                    statusChunks[i] = (byte) ChunkState.DOES_NOT_EXIST.ordinal();

                    allSuccessful = false;
                } else {
                    // put successful
                    statusChunks[i] = (byte) ChunkState.OK.ordinal();
                }

                if (m_backup.isActive()) {
                    // sort by backup peers
                    BackupRange backupRange = m_backup.getBackupRange(chunkIDs[i]);
                    ArrayList<ChunkAnon> remoteChunksOfBackupRange = remoteChunksByBackupRange.computeIfAbsent(backupRange, k -> new ArrayList<>());
                    remoteChunksOfBackupRange.add(new ChunkAnon(chunkIDs[i], data[i]));
                }
            }
        } finally {
            m_memoryManager.unlockAccess();
        }

        // unlock chunks
        if (p_request.getUnlockOperation() != ChunkLockOperation.NO_LOCK_OPERATION) {
            boolean writeLock = false;
            if (p_request.getUnlockOperation() == ChunkLockOperation.WRITE_LOCK) {
                writeLock = true;
            }

            for (long chunkID : chunkIDs) {
                m_lock.unlock(chunkID, m_boot.getNodeID(), writeLock);
            }
        }

        PutAnonResponse response;
        // cut message length if all were successful
        if (allSuccessful) {
            response = new PutAnonResponse(p_request, (byte) ChunkState.OK.ordinal());
        } else {
            // we got errors, default message
            response = new PutAnonResponse(p_request, statusChunks);
        }

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending chunk put respond to request %s failed: %s", p_request, e);
            // #endif /* LOGGER >= ERROR */
        }

        // Send backups
        if (m_backup.isActive()) {
            BackupRange backupRange;
            short[] backupPeers;
            ChunkAnon[] chunks;
            for (Map.Entry<BackupRange, ArrayList<ChunkAnon>> entry : remoteChunksByBackupRange.entrySet()) {
                backupRange = entry.getKey();
                chunks = entry.getValue().toArray(new ChunkAnon[entry.getValue().size()]);

                backupPeers = backupRange.getBackupPeers();
                for (short backupPeer : backupPeers) {
                    if (backupPeer != NodeID.INVALID_ID) {
                        // #if LOGGER == TRACE
                        LOGGER.trace("Logging %d chunks to 0x%X", chunks.length, backupPeer);
                        // #endif /* LOGGER == TRACE */

                        try {
                            m_network.sendMessage(new LogAnonMessage(backupPeer, backupRange.getRangeID(), chunks));
                        } catch (final NetworkException ignore) {

                        }
                    }
                }
            }
        }

        // #ifdef STATISTICS
        SOP_INCOMING_PUT_ANON.leave();
        // #endif /* STATISTICS */
    }
}