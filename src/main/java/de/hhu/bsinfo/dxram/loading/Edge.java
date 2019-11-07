package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public abstract class Edge extends AbstractChunk {

    private long m_sinkID;

    public Edge() {
    }

    public Edge(long p_chunkID, long m_sinkID) {
        super(p_chunkID);
        this.m_sinkID = m_sinkID;
    }

    public Edge(long p_sinkID) {
        m_sinkID = p_sinkID;
    }

    public long getSinkID() {
        return m_sinkID;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(m_sinkID);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_sinkID = p_importer.readLong(m_sinkID);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES;
    }

    @Override
    public String toString() {
        return "Edge to " + m_sinkID;
    }
}
