package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public abstract class Edge extends AbstractChunk {

    private long m_sourceID;
    private long m_sinkID;

    public Edge() {
    }

    public Edge(long p_sourceID, long p_sinkID) {
        this.m_sourceID = p_sourceID;
        this.m_sinkID = p_sinkID;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(m_sourceID);
        p_exporter.writeLong(m_sinkID);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_sourceID = p_importer.readLong(m_sourceID);
        m_sinkID = p_importer.readLong(m_sinkID);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES * 2;
    }
}
