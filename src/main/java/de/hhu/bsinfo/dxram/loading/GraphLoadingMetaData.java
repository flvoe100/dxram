package de.hhu.bsinfo.dxram.loading;

import java.util.List;

public class GraphLoadingMetaData {
    public List<Short> m_peers;
    public long[] m_startVertexRange;
    public long[] m_endVertexRange;

    public GraphLoadingMetaData(List<Short> p_peers) {
        this.m_peers = p_peers;
        this.m_startVertexRange = new long[p_peers.size()];
        this.m_endVertexRange = new long[p_peers.size()];
    }

    public List<Short> getPeers() {
        return m_peers;
    }

    public void changeVertexSpaceOfNode(short p_nodeID, long p_newStart, long p_newEnd) {
        int p_nodePos = getNodePos(p_nodeID);
        assert p_nodePos != -1;
        m_startVertexRange[p_nodePos] = p_newStart;
        m_endVertexRange[p_nodePos] = p_newEnd;
    }

    private int getNodePos(short p_nodeID) {
        for (int i = 0; i < m_peers.size(); i++) {
            if (m_peers.get(i) == p_nodeID) {
                return i;
            }
        }
        return -1;
    }
}
