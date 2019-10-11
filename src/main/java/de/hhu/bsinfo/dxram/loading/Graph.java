package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class Graph extends AbstractChunk {
    private boolean m_isDirected;
    private int m_numberOfVertices;
    private int m_numberOfEdges;

    public Graph() {
    }

    public boolean isM_isDirected() {
        return m_isDirected;
    }

    public void setIsDirected(boolean m_isDirected) {
        this.m_isDirected = m_isDirected;
    }

    public int getNumberOfVertices() {
        return m_numberOfVertices;
    }

    public void setNumberOfVertices(int m_numberOfVertices) {
        this.m_numberOfVertices = m_numberOfVertices;
    }

    public int getM_numberOfEdges() {
        return m_numberOfEdges;
    }

    public void setNumberOfEdges(int m_numberOfEdges) {
        this.m_numberOfEdges = m_numberOfEdges;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeBoolean(m_isDirected);
        p_exporter.writeInt(m_numberOfVertices);
        p_exporter.writeInt(m_numberOfEdges);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_isDirected = p_importer.readBoolean(m_isDirected);
        m_numberOfVertices = p_importer.readInt(m_numberOfVertices);
        m_numberOfEdges = p_importer.readInt(m_numberOfEdges);

    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 2 + ObjectSizeUtil.sizeofBoolean();
    }
}
