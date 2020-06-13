package de.hhu.bsinfo.dxram.loading;

public class Edge {

    private long m_sourceID;
    private long m_destID;

    public Edge() {
    }

    public Edge(long p_sourceID, long p_destID) {
        this.m_sourceID = p_sourceID;
        this.m_destID = p_destID;
    }


    public long getSourceID() {
        return  m_sourceID;
    }

    public long getDestID() {
        return  m_destID;
    }
}
