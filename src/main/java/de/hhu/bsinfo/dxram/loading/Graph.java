package de.hhu.bsinfo.dxram.loading;

public class Graph {
    private boolean m_isDirected;
    private int m_numberOfVertices;
    private int m_numberOfEdges;

    public Graph() {
    }

    public boolean isM_isDirected() {
        return m_isDirected;
    }

    public void setM_isDirected(boolean m_isDirected) {
        this.m_isDirected = m_isDirected;
    }

    public int getM_numberOfVertices() {
        return m_numberOfVertices;
    }

    public void setM_numberOfVertices(int m_numberOfVertices) {
        this.m_numberOfVertices = m_numberOfVertices;
    }

    public int getM_numberOfEdges() {
        return m_numberOfEdges;
    }

    public void setM_numberOfEdges(int m_numberOfEdges) {
        this.m_numberOfEdges = m_numberOfEdges;
    }
}
