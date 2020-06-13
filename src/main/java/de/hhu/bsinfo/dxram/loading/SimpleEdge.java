package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class SimpleEdge extends AbstractChunk {
    private long m_destID;

    public SimpleEdge() {

    }

    public SimpleEdge(long p_destID) {
        this.m_destID = p_destID;
    }

    public long getDestID(boolean cid) {
        return cid ? m_destID : ChunkID.getLocalID(m_destID);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(m_destID);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_destID = p_importer.readLong(m_destID);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES;
    }

    @Override
    public String toString() {
        return "SimpleEdge{" +
                "m_destID=" + m_destID +
                '}';
    }
}
