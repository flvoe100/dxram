package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.List;

public class Graph extends AbstractChunk {
    private boolean m_isDirected;
    private int[] m_numberOfVertices;
    private int[] m_numberOfEdges;
    private short[] m_slaveIDs;
    private short m_masterNodeID;

    public Graph() {
    }

    public Graph(List<Short> slaveIDs, short p_masterNodeID) {
        this.m_numberOfVertices = new int[slaveIDs.size()];
        this.m_numberOfEdges = new int[slaveIDs.size()];
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

    public int getNumberOfVerticesOfSlave(short slaveID) {
        System.out.println("slaveID = " + slaveID);
        int pos = getNodePos(slaveID);
        System.out.println("pos = " + pos);
        assert pos != -1;
        return m_numberOfVertices[pos];
    }

    public void setNumberOfVertices(int[] m_numberOfVertices) {
        this.m_numberOfVertices = m_numberOfVertices;
    }

    public int getNumberOfEdgesOfSlave(short slaveID) {
        int pos = getNodePos(slaveID);
        assert pos != -1;
        return m_numberOfEdges[pos];
    }

    public void setNumberOfEdges(int[] m_numberOfEdges) {
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
            System.out.println(m_slaveIDs[i]);
            if (m_slaveIDs[i] == p_nodeID) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeBoolean(m_isDirected);
        p_exporter.writeIntArray(m_numberOfVertices);
        p_exporter.writeIntArray(m_numberOfEdges);
        p_exporter.writeShort(m_masterNodeID);
        p_exporter.writeShortArray(m_slaveIDs);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_isDirected = p_importer.readBoolean(m_isDirected);
        m_numberOfVertices = p_importer.readIntArray(m_numberOfVertices);
        m_numberOfEdges = p_importer.readIntArray(m_numberOfEdges);
        m_masterNodeID = p_importer.readShort(m_masterNodeID);
        m_slaveIDs = p_importer.readShortArray(m_slaveIDs);

    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofIntArray(m_numberOfVertices) + ObjectSizeUtil.sizeofIntArray(m_numberOfEdges)
                + ObjectSizeUtil.sizeofBoolean() + Short.BYTES + ObjectSizeUtil.sizeofShortArray(m_slaveIDs);
    }
}
