package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ElementNotFoundException;
import de.hhu.bsinfo.dxutils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class DistributedLinkedByteList<T extends AbstractChunk> {

    private static final int LOCK_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(1);
    private static final int LOOKUP_TIMEOUT = (int) TimeUnit.MILLISECONDS.toMillis(500);

    private final MetaData metaData;
    private final DataHolder current;
    private final DataHolder previous;

    private final ChunkService chunkService;
    private ByteBufferImExporter exporter;
    private ByteBuffer byteBuffer;
    private Supplier<T> m_supplier;

    //in bytes
    private static final int MAXIMUM_STORAGE_SIZE = 128000;

    public DistributedLinkedByteList() {
        metaData = new MetaData();
        current = new DataHolder();
        previous = new DataHolder();
        chunkService = null;
    }

    public DistributedLinkedByteList(final MetaData metaData, final ChunkService chunkService, final Supplier<T> p_supplier) {
        this.metaData = metaData;
        this.chunkService = chunkService;
        m_supplier = p_supplier;
        current = new DataHolder();
        previous = new DataHolder();
    }

    public void add(short nodeId, T... values) {
        syncedWrite(() -> {
            add2(values, nodeId);
            return null;
        });
    }

    private void add2(T[] values, short nodeId) {
        if (current.m_count == 0) {
            fillUpCurrentListEntry2(values, nodeId);
        } else {
            createNewListEntry2(nodeId, values);
        }
        metaData.numberOfObjects += values.length;

    }

    private void add(T[] values, short nodeId) {
        if (MAXIMUM_STORAGE_SIZE - (current.m_count * getGenericSize()) >= Util.sizeOfObjectArray(values)) {
            fillUpCurrentListEntry(values, nodeId);
        } else {
            //create new linked entry
            int restBufferSize = MAXIMUM_STORAGE_SIZE - (current.m_count * m_supplier.get().sizeofObject());
            int fillUpSize = 0;
            if (restBufferSize != 0) {
                //first fillUp
                fillUpSize = restBufferSize / m_supplier.get().sizeofObject();
                fillUpCurrentListEntry(Arrays.copyOf(values, fillUpSize), nodeId);
            }
            int restValuesSize = values.length - fillUpSize;
            int dataHoldersToCreate = (int) Math.ceil((restValuesSize * getGenericSize()) / (double) MAXIMUM_STORAGE_SIZE);
            int numberOfObjects;
            for (int i = 0; i < dataHoldersToCreate; i++) {
                current.reset();
                numberOfObjects = Math.min(restValuesSize, getMaximumSavingObject());

                int startIndex = fillUpSize + (i * getMaximumSavingObject());
                int endIndex = startIndex + numberOfObjects;

                T[] restObjects = Arrays.copyOfRange(values, startIndex, endIndex);

                createNewListEntry(nodeId, restObjects);
                restValuesSize -= restObjects.length;
            }
        }
        metaData.numberOfObjects += values.length;
    }

    private void createNewListEntry(short nodeId, T... listEntryEntries) {
        int valuesSize = Util.sizeOfObjectArray(listEntryEntries);
        //Then create new with the rest
        byteBuffer = ByteBuffer.allocate(valuesSize);
        exporter = new ByteBufferImExporter(byteBuffer);
        for (T val : listEntryEntries) {
            //put entries in current values
            exporter.exportObject(val);
        }
        current.reset();
        boolean success = current.add(byteBuffer.array());
        if (!success) {
            System.err.println("Adding values to current linked item failed!");
        }
        current.m_count += listEntryEntries.length;
        current.setNext(ChunkID.INVALID_ID);
        int created = chunkService.create().create(nodeId, current);
        if (created != 1) {
            System.err.println("Creating a new linked list entry failed");
        }
        if (metaData.head == ChunkID.INVALID_ID) {
            metaData.head = current.getID();
            metaData.tail = metaData.head;
        } else {
            previous.reset();
            previous.setID(metaData.tail);
            chunkService.get().get(previous);
            previous.setNext(current.getID());
            chunkService.put().put(previous);
            metaData.tail = current.getID();
        }

        success = chunkService.put().put(current);
        if (!success) {
            System.err.println("putting a new linked list entry failed");
        }
        metaData.listSize += 1;
    }

    private void createNewListEntry2(short nodeId, T... listEntryEntries) {
        int valuesSize = Util.sizeOfObjectArray(listEntryEntries);
        //Then create new with the rest
        byteBuffer = ByteBuffer.allocate(valuesSize);
        exporter = new ByteBufferImExporter(byteBuffer);
        for (T val : listEntryEntries) {
            //put entries in current values
            exporter.exportObject(val);
        }
        current.reset();
        current.add2(byteBuffer.array());

        current.m_count += listEntryEntries.length;
        current.setNext(ChunkID.INVALID_ID);
        int created = chunkService.create().create(nodeId, current);
        if (created != 1) {
            System.err.println("Creating a new linked list entry failed");
        }
        if (metaData.head == ChunkID.INVALID_ID) {
            metaData.head = current.getID();
            metaData.tail = metaData.head;
        } else {
            previous.reset();
            previous.setID(metaData.tail);
            chunkService.get().get(previous);
            previous.setNext(current.getID());
            chunkService.put().put(previous);
            metaData.tail = current.getID();
        }

        boolean success = chunkService.put().put(current);
        if (!success) {
            System.err.println("putting a new linked list entry failed");
        }
        metaData.listSize += 1;
    }

    private void fillUpCurrentListEntry(T[] values, final short nodeId) {
        int valuesSize = Util.sizeOfObjectArray(values);
        byteBuffer = ByteBuffer.allocate(valuesSize);
        exporter = new ByteBufferImExporter(byteBuffer);
        for (T val : values) {
            //put entries in current values
            exporter.exportObject(val);
        }
        boolean success = current.add(byteBuffer.array());
        if (!success) {
            System.err.println("Adding values to current linked item failed!");
        }
        current.m_count += values.length;

        if (current.getID() == ChunkID.INVALID_ID) {
            int created = chunkService.create().create(nodeId, current);
            if (created != 1) {
                System.err.println("Creating a new linked list entry failed");
            }
        }

        if (metaData.head == ChunkID.INVALID_ID) {
            metaData.head = current.getID();
            metaData.tail = metaData.head;
            metaData.listSize += 1;
        }

        success = chunkService.put().put(current);
        if (!success) {
            System.err.println("Putting a entry failed");
        }
    }

    private void fillUpCurrentListEntry2(T[] values, final short nodeId) {
        int valuesSize = Util.sizeOfObjectArray(values);
        byteBuffer = ByteBuffer.allocate(valuesSize);
        exporter = new ByteBufferImExporter(byteBuffer);
        for (T val : values) {
            //put entries in current values
            exporter.exportObject(val);
        }
        current.add2(byteBuffer.array());

        current.m_count += values.length;

        if (current.getID() == ChunkID.INVALID_ID) {
            int created = chunkService.create().create(nodeId, current);
            if (created != 1) {
                System.err.println("Creating a new linked list entry failed");
            }
        }

        if (metaData.head == ChunkID.INVALID_ID) {
            metaData.head = current.getID();
            metaData.tail = metaData.head;
            metaData.listSize += 1;
        }

        boolean success = chunkService.put().put(current);
        if (!success) {
            System.err.println("Putting a entry failed");
        }
    }

    private int getGenericSize() {
        return m_supplier.get().sizeofObject();
    }

    private int getMaximumSavingObject() {
        int genericSize = getGenericSize();
        return MAXIMUM_STORAGE_SIZE / genericSize;
    }

    public List<T> getAll() {

        return syncedRead(() -> {

            List<T> list = new ArrayList<>();
            long nextId = metaData.head;
            T val;
            for (long l = 0; l < metaData.listSize; l++) {
                current.reset();
                current.setID(nextId);
                chunkService.get().get(current);
                exporter = new ByteBufferImExporter(ByteBuffer.wrap(current.m_values));
                for (int i = 0; i < current.m_count; i++) {
                    val = m_supplier.get();
                    exporter.importObject(val);
                    list.add(val);
                }
                nextId = current.getNext();
            }

            return list;
        });
    }

    public long size() {
        return syncedRead(() -> metaData.numberOfObjects);
    }

    public boolean isEmpty() {
        return syncedRead(() -> metaData.head == ChunkID.INVALID_ID);
    }

    private void ensureSynced() {
        chunkService.get().get(metaData);
    }

    private void updateMetaData() {
        chunkService.put().put(metaData);
    }

    private void acquireReadLock() {
        int chunksLocked = 0;
        while (chunksLocked != 1) {
            chunksLocked = chunkService.lock().lock(true, false, LOCK_TIMEOUT, metaData);
        }
    }

    private void releaseReadLock() {
        int chunksUnlocked = 0;
        while (chunksUnlocked != 1) {
            chunksUnlocked = chunkService.lock().lock(false, false, LOCK_TIMEOUT, metaData);
        }
    }

    private void acquireWriteLock() {
        int chunksLocked = 0;
        while (chunksLocked != 1) {
            chunksLocked = chunkService.lock().lock(true, true, LOCK_TIMEOUT, metaData);
        }
    }

    private void releaseWriteLock() {
        int chunksUnlocked = 0;
        while (chunksUnlocked != 1) {
            chunksUnlocked = chunkService.lock().lock(false, true, LOCK_TIMEOUT, metaData);
        }
    }

    private <S> S syncedRead(Supplier<S> supplier) {
        try {
            acquireReadLock();
            ensureSynced();
            return supplier.get();
        } finally {
            releaseReadLock();
        }
    }

    private <S> S syncedWrite(Supplier<S> supplier) {
        try {
            acquireWriteLock();
            ensureSynced();
            S ret = supplier.get();
            updateMetaData();
            return ret;
        } finally {
            releaseWriteLock();
        }
    }

    public long getMetaDataID() {
        return metaData.getID();
    }

    /**
     * Retrieves an existing distributed linked list using the specified name.
     */
    public static <T extends AbstractChunk> DistributedLinkedByteList<T> get(final long p_metaDataID, final ChunkService chunkService, final Supplier<T> valueSupplier) {
        if (p_metaDataID == ChunkID.INVALID_ID) {
            throw new ElementNotFoundException(String.format("Chunk has invalid ChunkID"));
        }

        MetaData metaData = new MetaData(p_metaDataID);
        if (!chunkService.get().get(metaData)) {
            throw new ElementNotFoundException(String.format("Chunk wit id %08X not found", p_metaDataID));
        }
        return new DistributedLinkedByteList<>(metaData, chunkService, valueSupplier);
    }

    /**
     * Creates a new distributed linked list using the specified name if it doesn't exist yet.
     */
    public static <T extends AbstractChunk> DistributedLinkedByteList<T> create(final long metaDataID, final ChunkService chunkService, final Supplier<T> valueSupplier) {
        MetaData metaData = new MetaData();
        metaData.setID(metaDataID);
        chunkService.put().put(metaData);

        return new DistributedLinkedByteList<>(metaData, chunkService, valueSupplier);
    }

    /**
     * Creates a new distributed linked list using the specified name if it doesn't exist yet.
     */
    public static <T extends AbstractChunk> DistributedLinkedByteList<T> create(final ChunkLocalService chunkLocalService, final ChunkService chunkService, final Supplier<T> valueSupplier) {
        MetaData metaData = new MetaData();
        int created = chunkLocalService.createLocal().create(metaData);
        if (created != 1) {
            System.err.println("Creating id for metadata of distributed linked list failed!");
        }
        boolean success = chunkService.put().put(metaData);
        if (!success) {
            System.err.println("Failed to put metadata");
        }
        return new DistributedLinkedByteList<>(metaData, chunkService, valueSupplier);
    }


    private static final class MetaData extends AbstractChunk {
        private long head = ChunkID.INVALID_ID;
        private long tail = ChunkID.INVALID_ID;
        private long listSize;
        private long numberOfObjects;

        private MetaData() {
        }

        private MetaData(long chunkID) {
            super(chunkID);
        }

        @Override
        public void exportObject(Exporter exporter) {
            exporter.writeLong(head);
            exporter.writeLong(tail);
            exporter.writeLong(listSize);
            exporter.writeLong(numberOfObjects);
        }

        @Override
        public void importObject(Importer importer) {
            head = importer.readLong(head);
            tail = importer.readLong(tail);
            listSize = importer.readLong(listSize);
            numberOfObjects = importer.readLong(numberOfObjects);
        }

        @Override
        public int sizeofObject() {
            return 4 * Long.BYTES;
        }
    }

    private static final class DataHolder extends AbstractChunk {

        private byte[] m_values;
        private int m_count;
        private int m_putPointer;
        private long next = ChunkID.INVALID_ID;

        private DataHolder() {
            m_values = new byte[MAXIMUM_STORAGE_SIZE];
            m_count = 0;
            m_putPointer = 0;
        }

        public boolean add(byte[] values) {
            if (m_values.length - m_putPointer >= values.length) {
                for (int i = 0; i < values.length; i++) {
                    m_values[m_putPointer] = values[i];
                    m_putPointer++;
                }
                return true;

            }
            return false;

        }

        public void add2(byte[] values) {
            m_values = values;


        }

        @Override
        public void exportObject(Exporter exporter) {
            exporter.writeByteArray(m_values);
            exporter.writeLong(next);
            exporter.writeInt(m_count);
            exporter.writeInt(m_putPointer);
        }

        @Override
        public void importObject(Importer importer) {
            m_values = importer.readByteArray(m_values);
            next = importer.readLong(next);
            m_count = importer.readInt(m_count);
            m_putPointer = importer.readInt(m_putPointer);
        }

        @Override
        public int sizeofObject() {
            return ObjectSizeUtil.sizeofByteArray(m_values) + Long.BYTES + 2 * Integer.BYTES;
        }

        int sizeOfByteValues() {
            return ObjectSizeUtil.sizeofByteArray(m_values);
        }

        long getNext() {
            return next;
        }

        private void setNext(long next) {
            this.next = next;
        }

        private void reset() {
            next = ChunkID.INVALID_ID;
            m_count = 0;
            m_putPointer = 0;
            m_values = new byte[MAXIMUM_STORAGE_SIZE];
            setID(ChunkID.INVALID_ID);
            setState(ChunkState.UNDEFINED);
        }

    }
}
