package de.hhu.bsinfo.dxram.lookup.overlay.storage;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.lookup.overlay.OverlayHelper;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Table managing synchronization barriers on a superpeer.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 06.05.2016
 */
public class BarriersTable extends AbstractMetadata {

    private static final Logger LOGGER = LogManager.getFormatterLogger(BarriersTable.class.getSimpleName());

    private int m_maxNumBarriers;
    private int m_allBarriersCount;
    private int m_allBarrierEntriesCount;

    private BarrierNode[] m_barrierNodes;
    private ReentrantLock m_allocationLock;

    /**
     * Constructor
     *
     * @param p_maxNumBarriers
     *         Max number of barriers allowed to be allocated.
     * @param p_nodeId
     *         Node id of the superpeer this class is running on.
     */
    public BarriersTable(final int p_maxNumBarriers, final short p_nodeId) {
        m_maxNumBarriers = p_maxNumBarriers;
        m_allBarriersCount = 0;
        m_allBarrierEntriesCount = 0;

        m_barrierNodes = new BarrierNode[Short.MAX_VALUE * 2];

        m_allocationLock = new ReentrantLock(false);
    }

    @Override public int storeMetadata(final byte[] p_data, final int p_offset, final int p_size) {
        int ret = 0;
        short nodeId;
        int allSize;
        int size;
        long[][] barrierData = null;
        short[][] barrierState;
        ByteBuffer data;

        data = ByteBuffer.wrap(p_data, p_offset, p_size);
        while (data.position() < data.limit()) {
            nodeId = data.getShort();
            allSize = data.getInt();
            barrierData = new long[m_maxNumBarriers][];
            barrierState = new short[m_maxNumBarriers][];

            // #if LOGGER == TRACE
            LOGGER.trace("Storing barriers of 0x%X", nodeId);
            // #endif /* LOGGER == TRACE */

            for (int i = 0; i < allSize; i++) {
                size = data.getInt();
                barrierData[i] = new long[size];
                barrierState[i] = new short[size + 1];

                for (int j = 0; j < size; j++) {
                    barrierData[i][j] = data.getLong();
                    barrierState[i][j] = data.getShort();
                }
                barrierState[i][allSize] = data.getShort();
            }

            m_barrierNodes[nodeId & 0xFFFF] = new BarrierNode(nodeId, m_maxNumBarriers, barrierData, barrierState);
            ret += allSize;
        }

        return ret;
    }

    @Override public byte[] receiveAllMetadata() {
        BarrierNode barrierNode;

        int size = m_allBarrierEntriesCount * (Long.BYTES + Short.SIZE) + m_allBarriersCount * Short.SIZE;
        ByteBuffer data = ByteBuffer.allocate(size);

        for (int i = 0; i < m_barrierNodes.length; i++) {
            barrierNode = m_barrierNodes[i & 0xFFFF];
            if (barrierNode != null) {
                // #if LOGGER == TRACE
                LOGGER.trace("Including barriers of 0x%X", (short) i);
                // #endif /* LOGGER == TRACE */

                data.put(barrierNode.toByteArray());
            }
        }

        return data.array();
    }

    @Override public byte[] receiveMetadataInRange(final short p_bound1, final short p_bound2) {
        int currentSize = 0;
        byte[] currentData;
        BarrierNode barrierNode;

        int size = m_allBarrierEntriesCount * (Long.BYTES + Short.SIZE) + m_allBarriersCount * Short.SIZE;
        ByteBuffer data = ByteBuffer.allocate(size);

        for (int i = 0; i < m_barrierNodes.length; i++) {
            if (OverlayHelper.isPeerInSuperpeerRange((short) i, p_bound1, p_bound2)) {
                barrierNode = m_barrierNodes[i & 0xFFFF];
                if (barrierNode != null) {
                    // #if LOGGER == TRACE
                    LOGGER.trace("Including barriers of 0x%X", (short) i);
                    // #endif /* LOGGER == TRACE */

                    currentData = barrierNode.toByteArray();
                    data.put(currentData);
                    currentSize += currentData.length;
                }
            }
        }

        return Arrays.copyOfRange(data.array(), 0, currentSize);
    }

    @Override public int removeMetadataOutsideOfRange(final short p_bound1, final short p_bound2) {
        int ret = 0;
        int res = 0;
        int counter = 0;
        BarrierNode node;

        for (int i = 0; i < m_barrierNodes.length; i++) {
            if (!OverlayHelper.isPeerInSuperpeerRange((short) i, p_bound1, p_bound2)) {
                while (res != -1) {
                    node = m_barrierNodes[i & 0xFFFF];
                    if (node == null) {
                        break;
                    }

                    // #if LOGGER == TRACE
                    LOGGER.trace("Removing barriers of 0x%X", (short) i);
                    // #endif /* LOGGER == TRACE */

                    res = node.freeBarrier(counter++);
                    if (res != -1) {
                        m_allBarriersCount--;
                        m_allBarrierEntriesCount -= res;
                    }
                }
                m_barrierNodes[i & 0xFFFF] = null;
                ret += counter;
                counter = 0;
            }
        }

        return ret;
    }

    @Override public int quantifyMetadata(final short p_bound1, final short p_bound2) {
        int count = 0;
        BarrierNode barrierNode;

        for (int i = 0; i < m_barrierNodes.length; i++) {
            if (OverlayHelper.isPeerInSuperpeerRange((short) i, p_bound1, p_bound2)) {
                barrierNode = m_barrierNodes[i & 0xFFFF];
                if (barrierNode != null) {
                    count += barrierNode.getNumberOfBarriers();
                }
            }
        }

        return count;
    }

    /**
     * Allocate a new barrier.
     *
     * @param p_nodeId
     *         the creator
     * @param p_size
     *         Size of the barrier, i.e. how many peers have to sign on for release.
     * @return Barrier id on succuess, -1 on failure.
     */
    public int allocateBarrier(final short p_nodeId, final int p_size) {
        int ret;

        if (m_barrierNodes[p_nodeId] == null) {
            m_barrierNodes[p_nodeId] = new BarrierNode(p_nodeId, m_maxNumBarriers);
        }
        ret = m_barrierNodes[p_nodeId].allocateBarrier(p_size);

        if (ret != -1) {
            m_allBarriersCount++;
            m_allBarrierEntriesCount += p_size;
        }

        return ret;
    }

    /**
     * Free a previously allocated barrier.
     *
     * @param p_nodeId
     *         the creator
     * @param p_barrierId
     *         Id of the barrier to free.
     * @return True if successful, false on failure.
     */
    public boolean freeBarrier(final short p_nodeId, final int p_barrierId) {
        int ret;

        ret = m_barrierNodes[p_nodeId].freeBarrier(p_barrierId);
        if (ret != -1) {
            m_allBarriersCount--;
            m_allBarrierEntriesCount -= ret;
        }

        return ret != -1;
    }

    /**
     * Change the size of a barrier after being created (i.e. you want to keep the barrier id)
     *
     * @param p_nodeId
     *         the creator
     * @param p_barrierId
     *         Id of the barrier to change the size of.
     * @param p_newSize
     *         The new size for the barrier.
     * @return True if chaning size was sucessful, false otherwise.
     */
    public boolean changeBarrierSize(final short p_nodeId, final int p_barrierId, final int p_newSize) {
        return m_barrierNodes[p_nodeId].changeBarrierSize(p_barrierId, p_newSize);
    }

    /**
     * Sign on to a barrier using a barrier id.
     *
     * @param p_nodeId
     *         the creator
     * @param p_barrierId
     *         Barrier id to sign on to.
     * @param p_nodeIdToSignOn
     *         Id of the peer node signing on
     * @param p_barrierData
     *         Additional custom data to pass along to the barrier
     * @return On success returns the number of peers left to sign on, -1 on failure
     */
    public int signOn(final short p_nodeId, final int p_barrierId, final short p_nodeIdToSignOn, final long p_barrierData) {
        return m_barrierNodes[p_nodeId].signOn(p_barrierId, p_nodeIdToSignOn, p_barrierData);
    }

    /**
     * Reset an existing barrier for reuse.
     *
     * @param p_nodeId
     *         the creator
     * @param p_barrierId
     *         Id of the barrier to reset.
     * @return True if successful, false otherwise.
     */
    public boolean reset(final short p_nodeId, final int p_barrierId) {
        return m_barrierNodes[p_nodeId].reset(p_barrierId);
    }

    /**
     * Get the list of currently signed on peers from a barrier.
     * The first item (index 0) is the sign on count.
     *
     * @param p_nodeId
     *         the creator
     * @param p_barrierId
     *         Id of the barrier to get.
     * @return Array with node ids that already signed on. First index element is the count of signed on peers.
     */
    public short[] getSignedOnPeers(final short p_nodeId, final int p_barrierId) {
        return m_barrierNodes[p_nodeId].getSignedOnPeers(p_barrierId);
    }

    /**
     * Get the custom data of a barrier that is passed along on barrier sign ons.
     *
     * @param p_nodeId
     *         the creator
     * @param p_barrierId
     *         Id of the barrier to get the custom data of.
     * @return On success returns an array with the currently available custom data (sorted by order the peers logged in)
     */
    public long[] getBarrierCustomData(final short p_nodeId, final int p_barrierId) {
        return m_barrierNodes[p_nodeId].getBarrierCustomData(p_barrierId);
    }

    /**
     * Wrapper class for all barriers of one superpeer
     *
     * @author Kevin Beineke, kevin.beineke@hhu.de, 10.10.2016
     */
    private final class BarrierNode {

        // Attributes
        private short m_nodeId;
        private long[][] m_barrierData;
        private short[][] m_barrierState;
        private ReentrantLock[] m_barrierLocks;
        private int m_allocatedBarriersCount;

        // Constructors

        /**
         * Creates an instance of BarrierNode
         *
         * @param p_nodeId
         *         the creator
         * @param p_maxNumBarriers
         *         Max number of barriers allowed to be allocated.
         */
        private BarrierNode(final short p_nodeId, final int p_maxNumBarriers) {
            m_barrierData = new long[p_maxNumBarriers][];
            m_barrierState = new short[p_maxNumBarriers][];
            m_barrierLocks = new ReentrantLock[p_maxNumBarriers];
            m_allocatedBarriersCount = 0;

            m_allocationLock = new ReentrantLock(false);

            m_nodeId = p_nodeId;
        }

        /**
         * Creates an instance of BarrierNode
         *
         * @param p_nodeId
         *         the creator
         * @param p_maxNumBarriers
         *         Max number of barriers allowed to be allocated.
         * @param p_barrierData
         *         all barriers' data
         * @param p_barrierState
         *         all barriers' states
         */
        private BarrierNode(final short p_nodeId, final int p_maxNumBarriers, final long[][] p_barrierData, final short[][] p_barrierState) {
            m_barrierData = p_barrierData;
            m_barrierState = p_barrierState;
            m_barrierLocks = new ReentrantLock[p_maxNumBarriers];
            m_allocatedBarriersCount = m_barrierData.length;

            m_allocationLock = new ReentrantLock(false);

            m_nodeId = p_nodeId;
        }

        // Methods

        /**
         * Returns the number of barriers in this barrier node
         *
         * @return the number of allocated barriers
         */
        private int getNumberOfBarriers() {
            return m_allocatedBarriersCount;
        }

        /**
         * Allocate a new barrier.
         *
         * @param p_size
         *         Size of the barrier, i.e. how many peers have to sign on for release.
         * @return Barrier id on succuess, -1 on failure.
         */
        private int allocateBarrier(final int p_size) {
            if (p_size < 1) {
                return BarrierID.INVALID_ID;
            }

            m_allocationLock.lock();

            // find next available barrier
            for (int i = 0; i < m_barrierState.length; i++) {
                if (m_barrierState[i] == null) {
                    m_barrierData[i] = new long[p_size];
                    m_barrierState[i] = new short[p_size + 1];
                    m_barrierLocks[i] = new ReentrantLock(false);
                    m_barrierState[i][0] = 0;
                    for (int j = 1; j < p_size; j++) {
                        m_barrierState[i][j] = NodeID.INVALID_ID;
                    }
                    m_allocatedBarriersCount++;
                    m_allocationLock.unlock();
                    return BarrierID.createBarrierId(m_nodeId, i);
                }
            }

            m_allocationLock.unlock();
            return BarrierID.INVALID_ID;
        }

        /**
         * Free a previously allocated barrier.
         *
         * @param p_barrierId
         *         Id of the barrier to free.
         * @return The size of freed barrier if successful, -1 on failure.
         */
        private int freeBarrier(final int p_barrierId) {
            int ret;

            if (p_barrierId == BarrierID.INVALID_ID) {
                return -1;
            }

            short nodeId = BarrierID.getOwnerID(p_barrierId);
            int id = BarrierID.getBarrierID(p_barrierId);

            if (nodeId != m_nodeId) {
                return -1;
            }

            if (id >= m_barrierState.length) {
                return -1;
            }

            if (m_allocationLock == null) {
                return -1;
            }

            m_allocationLock.lock();

            if (m_barrierState[id] == null) {
                m_allocationLock.unlock();
                return -1;
            }

            m_barrierLocks[id].lock();
            ret = m_barrierData[id].length;

            m_barrierData[id] = null;
            m_barrierState[id] = null;
            m_barrierLocks[id].unlock();

            m_barrierLocks[id] = null;
            m_allocatedBarriersCount--;

            m_allocationLock.unlock();

            return ret;
        }

        /**
         * Change the size of a barrier after being created (i.e. you want to keep the barrier id)
         *
         * @param p_barrierId
         *         Id of the barrier to change the size of.
         * @param p_newSize
         *         The new size for the barrier.
         * @return True if chaning size was sucessful, false otherwise.
         */
        private boolean changeBarrierSize(final int p_barrierId, final int p_newSize) {
            if (p_barrierId == BarrierID.INVALID_ID) {
                return false;
            }

            if (p_newSize < 1) {
                return false;
            }

            short nodeId = BarrierID.getOwnerID(p_barrierId);
            int id = BarrierID.getBarrierID(p_barrierId);

            if (nodeId != m_nodeId) {
                return false;
            }

            if (m_barrierState[id] == null) {
                return false;
            }

            m_barrierLocks[id].lock();
            // cannot change size if barrier is currently in use
            if (m_barrierState[id][0] != 0) {
                m_barrierLocks[id].unlock();
                return false;
            }

            m_barrierData[id] = new long[p_newSize];
            m_barrierState[id] = new short[p_newSize + 1];
            m_barrierState[id][0] = 0;
            for (int i = 1; i < m_barrierState[id].length; i++) {
                m_barrierState[id][i] = NodeID.INVALID_ID;
            }

            m_barrierLocks[id].unlock();
            return true;
        }

        /**
         * Sign on to a barrier using a barrier id.
         *
         * @param p_barrierId
         *         Barrier id to sign on to.
         * @param p_nodeId
         *         Id of the peer node signing on
         * @param p_barrierData
         *         Additional custom data to pass along to the barrier
         * @return On success returns the number of peers left to sign on, -1 on failure
         */
        private int signOn(final int p_barrierId, final short p_nodeId, final long p_barrierData) {
            if (p_barrierId == BarrierID.INVALID_ID) {
                return -1;
            }

            short nodeId = BarrierID.getOwnerID(p_barrierId);
            int id = BarrierID.getBarrierID(p_barrierId);

            if (nodeId != m_nodeId) {
                return -1;
            }

            if (id >= m_barrierState.length || m_barrierState[id] == null) {
                return -1;
            }

            m_barrierLocks[id].lock();

            if (m_barrierState[id][0] == m_barrierState[id].length - 1) {
                m_barrierLocks[id].unlock();
                return -1;
            }

            m_barrierData[id][(int) (m_barrierState[id][0] & 0xFFFF)] = p_barrierData;
            m_barrierState[id][0]++;
            m_barrierState[id][(int) (m_barrierState[id][0] & 0xFFFF)] = p_nodeId;

            int ret = (m_barrierState[id].length - 1) - m_barrierState[id][0];
            m_barrierLocks[id].unlock();
            return ret;
        }

        /**
         * Reset an existing barrier for reuse.
         *
         * @param p_barrierId
         *         Id of the barrier to reset.
         * @return True if successful, false otherwise.
         */
        private boolean reset(final int p_barrierId) {
            if (p_barrierId == BarrierID.INVALID_ID) {
                return false;
            }

            short nodeId = BarrierID.getOwnerID(p_barrierId);
            int id = BarrierID.getBarrierID(p_barrierId);

            if (nodeId != m_nodeId) {
                return false;
            }

            if (id >= m_barrierState.length || m_barrierState[id] == null) {
                return false;
            }

            m_barrierLocks[id].lock();

            m_barrierState[id][0] = 0;
            for (int i = 1; i < m_barrierState[id].length; i++) {
                m_barrierData[id][i - 1] = 0;
                m_barrierState[id][i] = NodeID.INVALID_ID;
            }

            m_barrierLocks[id].unlock();
            return true;
        }

        /**
         * Get the list of currently signed on peers from a barrier.
         * The first item (index 0) is the sign on count.
         *
         * @param p_barrierId
         *         Id of the barrier to get.
         * @return Array with node ids that already signed on. First index element is the count of signed on peers.
         */
        private short[] getSignedOnPeers(final int p_barrierId) {
            if (p_barrierId == BarrierID.INVALID_ID) {
                return null;
            }

            short nodeId = BarrierID.getOwnerID(p_barrierId);
            int id = BarrierID.getBarrierID(p_barrierId);

            if (nodeId != m_nodeId) {
                return null;
            }

            if (id >= m_barrierState.length || m_barrierState[id] == null) {
                return null;
            }

            return m_barrierState[id];
        }

        /**
         * Get the custom data of a barrier that is passed along on barrier sign ons.
         *
         * @param p_barrierId
         *         Id of the barrier to get the custom data of.
         * @return On success returns an array with the currently available custom data (sorted by order the peers logged in)
         */
        private long[] getBarrierCustomData(final int p_barrierId) {
            if (p_barrierId == BarrierID.INVALID_ID) {
                return null;
            }

            short nodeId = BarrierID.getOwnerID(p_barrierId);
            int id = BarrierID.getBarrierID(p_barrierId);

            if (nodeId != m_nodeId) {
                return null;
            }

            if (id >= m_barrierState.length || m_barrierState[id] == null) {
                return null;
            }

            return m_barrierData[id];
        }

        /**
         * Return all barriers in a byte array.
         *
         * @return a byte array with all barriers.
         */
        private byte[] toByteArray() {
            byte[] ret;
            int size = Short.BYTES + Integer.BYTES;
            ByteBuffer data;

            for (int i = 0; i < m_allocatedBarriersCount; i++) {
                size += m_barrierData[i].length * Long.BYTES;
                size += (m_barrierState[i].length + 1) * Short.BYTES;
                size += Integer.BYTES;
            }

            ret = new byte[size];
            data = ByteBuffer.wrap(ret);
            data.putShort(m_nodeId);
            data.putInt(m_barrierData.length);
            for (int i = 0; i < m_allocatedBarriersCount; i++) {
                data.putInt(m_barrierData[i].length);

                // #if LOGGER == TRACE
                LOGGER.trace("Including barrier with id %d", i);
                // #endif /* LOGGER == TRACE */

                for (int j = 0; j < m_barrierData[i].length; j++) {
                    data.putLong(m_barrierData[i][j]);
                    data.putShort(m_barrierState[i][j]);
                }
                data.putShort(m_barrierState[i][m_barrierState[i].length - 1]);
            }

            return ret;
        }
    }
}
