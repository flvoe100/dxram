package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.List;

public class Graph extends AbstractChunk {
    private boolean m_isDirected;
    private int m_numberOfVertices;
    private int m_numberOfEdges;
    private short[] m_slaveIDs;
    private short m_masterNodeID;

    public Graph() {
    }

    public Graph(List<Short> slaveIDs, short p_masterNodeID) {
        this.m_masterNodeID = p_masterNodeID;
        this.m_slaveIDs = new short[slaveIDs.size()];
        for (int i = 0; i < slaveIDs.size(); i++) {
            m_slaveIDs[i] = slaveIDs.get(i);
        }
    }

    public boolean isIsDirected() {
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

    public int getNumberOfEdges() {
        return m_numberOfEdges;
    }

    public void setNumberOfEdges(int m_numberOfEdges) {
        this.m_numberOfEdges = m_numberOfEdges;
    }

    public short getMasterNodeID() {
        return m_masterNodeID;
    }

    public void setMasterNodeID(short m_masterNodeID) {
        this.m_masterNodeID = m_masterNodeID;
    }

    private int getNodePos(short p_nodeID) {
        for (int i = 0; i < m_slaveIDs.length; i++) {
            if (m_slaveIDs[i] == p_nodeID) {
                return i;
            }
        }
        System.out.println(p_nodeID);
        return -1;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeBoolean(m_isDirected);
        p_exporter.writeInt(m_numberOfVertices);
        p_exporter.writeInt(m_numberOfEdges);
        p_exporter.writeShort(m_masterNodeID);
        p_exporter.writeShortArray(m_slaveIDs);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_isDirected = p_importer.readBoolean(m_isDirected);
        m_numberOfVertices = p_importer.readInt(m_numberOfVertices);
        m_numberOfEdges = p_importer.readInt(m_numberOfEdges);
        m_masterNodeID = p_importer.readShort(m_masterNodeID);
        m_slaveIDs = p_importer.readShortArray(m_slaveIDs);

    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 2 + ObjectSizeUtil.sizeofBoolean() + Short.BYTES + ObjectSizeUtil.sizeofShortArray(m_slaveIDs);
    }
}
