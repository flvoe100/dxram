package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.util.List;

public class Partition extends AbstractChunk {
    private long from;
    private long fromByteOffset;
    private long to;
    private long toByteOffset;
    private int partitionNumber;
    private short nodeID;

    public Partition(long from, long fromByteOffset, long to, long toByteOffset, int partitionNumber, short nodeID) {
        this.from = from;
        this.fromByteOffset = fromByteOffset;
        this.to = to;
        this.toByteOffset = toByteOffset;
        this.partitionNumber = partitionNumber;
        this.nodeID = nodeID;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }

    public long getFromByteOffset() {
        return fromByteOffset;
    }

    public short getNodeID() {
        return nodeID;
    }

    public static short getNodeIDOfVertix(List<Partition> partitions, long vertexID) {
        for (Partition p : partitions) {
            if (p.isBetween(vertexID)) {
                return p.getNodeID();
            }
        }
        return -1;

    }

    public long getToByteOffset() {
        return toByteOffset;
    }

    public boolean isBetween(long x) {
        return x >= from && x <= to;
    }

    @Override
    public void importObject(Importer p_importer) {
        from = p_importer.readLong(from);
        fromByteOffset = p_importer.readLong(fromByteOffset);
        to = p_importer.readLong(to);
        toByteOffset = p_importer.readLong(toByteOffset);
        partitionNumber = p_importer.readInt(partitionNumber);
        nodeID = p_importer.readShort(nodeID);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(from);
        p_exporter.writeLong(fromByteOffset);
        p_exporter.writeLong(to);
        p_exporter.writeLong(toByteOffset);
        p_exporter.writeInt(partitionNumber);
        p_exporter.writeShort(nodeID);
    }

    @Override
    public int sizeofObject() {
        return 4 * Long.BYTES + Integer.BYTES + Short.BYTES;
    }

    @Override
    public String toString() {
        return "Partition{" +
                "from=" + from +
                ", fromByteOffset=" + fromByteOffset +
                ", to=" + to +
                ", toByteOffset=" + toByteOffset +
                ", partitionNumber=" + partitionNumber +
                '}';
    }
}
