package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.List;

public class GraphLoadingMetaData extends AbstractChunk {
    public short[] m_peers;
    public long[] m_startVertexRange;
    public long[] m_endVertexRange;

    public GraphLoadingMetaData() {
    }

    public GraphLoadingMetaData(List<Short> p_peers) {
        this.m_peers = new short[p_peers.size()];
        for (int i = 0; i < p_peers.size(); i++) {
            m_peers[i] = p_peers.get(i);
        }
        this.m_startVertexRange = new long[p_peers.size()];
        this.m_endVertexRange = new long[p_peers.size()];
    }

    public short[] getPeers() {
        return m_peers;
    }

    public void changeVertexSpaceOfNode(short p_nodeID, long p_newStart, long p_newEnd) {
        int p_nodePos = getNodePos(p_nodeID);
        assert p_nodePos != -1;
        m_startVertexRange[p_nodePos] = p_newStart;
        m_endVertexRange[p_nodePos] = p_newEnd;
    }

    private int getNodePos(short p_nodeID) {
        for (int i = 0; i < m_peers.length; i++) {
            if (m_peers[i] == p_nodeID) {
                return i;
            }
        }
        System.out.println(p_nodeID);
        return -1;
    }

    public long getStartIntervalOfNode(short p_nodeID) {
        return m_startVertexRange[getNodePos(p_nodeID)];
    }

    public long getEndIntervalOfNode(short p_nodeID) {
        return m_endVertexRange[getNodePos(p_nodeID)];
    }

    @Override
    public void exportObject(Exporter p_exporter) {

        p_exporter.writeShortArray(m_peers);
        p_exporter.writeLongArray(m_startVertexRange);
        p_exporter.writeLongArray(m_endVertexRange);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_peers = p_importer.readShortArray(m_peers);
        m_startVertexRange = p_importer.readLongArray(m_startVertexRange);
        m_endVertexRange = p_importer.readLongArray(m_endVertexRange);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofShortArray(m_peers) + ObjectSizeUtil.sizeofLongArray(m_startVertexRange) + ObjectSizeUtil.sizeofLongArray(m_endVertexRange);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GraphLoadingMetaData\n")
                .append("Peers: [");
        for (short p_peer : m_peers) {
            sb.append(p_peer + ", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("]\nStartVertexIDs: [");
        for (long p_startVID : m_startVertexRange) {
            sb.append(p_startVID + ", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("]\nEndVertexIDs: [");
        for (long p_endVID : m_endVertexRange) {
            sb.append(p_endVID + ", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("]");
        return sb.toString();
    }
}
