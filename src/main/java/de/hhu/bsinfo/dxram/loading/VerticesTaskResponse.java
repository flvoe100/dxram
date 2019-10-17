package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;

public class VerticesTaskResponse extends Message {
    private short m_nodeID;
    private long m_startID;
    private long m_endID;

    public VerticesTaskResponse() {
    }

    public VerticesTaskResponse(short p_masterNodeID, short p_nodeID, long p_startID, long p_endID) {
        super(p_masterNodeID, DXRAMMessageTypes.GRAPH_LOADING_MESSAGE_TYPE, DxGraphMessageTypes.SUBTYPE_VERTICES_LOADING_TASK_RESPONSE);
        this.m_nodeID = p_nodeID;
        this.m_startID = p_startID;
        this.m_endID = p_endID;
    }

    public short getNodeID() {
        return m_nodeID;
    }

    public long getStartID() {
        return m_startID;
    }

    public long getEndID() {
        return m_endID;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_nodeID);
        p_exporter.writeLong(m_startID);
        p_exporter.writeLong(m_endID);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_nodeID = p_importer.readShort(m_nodeID);
        m_startID = p_importer.readLong(m_startID);
        m_endID = p_importer.readLong(m_endID);
    }

    @Override
    protected int getPayloadLength() {
        return Short.BYTES + 2 * Long.BYTES;
    }
}
