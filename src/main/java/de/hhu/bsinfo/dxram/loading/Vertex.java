package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class Vertex extends AbstractChunk {

    private long m_neighbourLinkedListID = ChunkID.INVALID_ID;
    private DistributedLinkedByteList<SimpleEdge> m_neighbourLinkedList = null;

    public Vertex() {
    }

    public Vertex(long p_chunkID) {
        super(p_chunkID);
    }

    public Vertex(long p_chunkID, long m_neighbourLinkedListID) {
        super(p_chunkID);
        this.m_neighbourLinkedListID = m_neighbourLinkedListID;
    }

    public DistributedLinkedByteList<SimpleEdge> getNeighbourLinkedList() {
        return m_neighbourLinkedList;
    }

    public void setNeighbourLinkedList(DistributedLinkedByteList<SimpleEdge> m_neighbourLinkedList) {
        this.m_neighbourLinkedList = m_neighbourLinkedList;
    }


    public long getVertexID() {
        return ChunkID.getLocalID(this.getID());
    }

    public long getNeighbourLinkedListID() {
        return m_neighbourLinkedListID;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(this.m_neighbourLinkedListID);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_neighbourLinkedListID = p_importer.readLong(m_neighbourLinkedListID);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES;
    }

    @Override
    public String toString() {
        return "Vertex{" +
                "m_neighbourLinkedListID=" + m_neighbourLinkedListID +
                '}';
    }
}
