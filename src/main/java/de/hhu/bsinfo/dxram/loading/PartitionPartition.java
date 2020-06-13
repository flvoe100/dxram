package de.hhu.bsinfo.dxram.loading;

public class PartitionPartition {
    private String from;
    private long fromByteOffset;
    private String to;
    private long toByteOffset;
    private int partitionNumber;

    public PartitionPartition(String from, long fromByteOffset, String to, long toByteOffset, int partitionNumber) {
        this.from = from;
        this.fromByteOffset = fromByteOffset;
        this.to = to;
        this.toByteOffset = toByteOffset;
        this.partitionNumber = partitionNumber;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }

    public long getFromByteOffset() {
        return fromByteOffset;
    }

    public long getToByteOffset() {
        return toByteOffset;
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
