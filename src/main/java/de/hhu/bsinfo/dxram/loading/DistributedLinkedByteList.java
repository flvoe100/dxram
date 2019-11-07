package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ElementAlreadyExistsException;
import de.hhu.bsinfo.dxram.data.ElementNotFoundException;
import de.hhu.bsinfo.dxram.engine.ServiceProvider;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class DistributedLinkedByteList<T extends AbstractChunk> {

    private static final int LOCK_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(1);
    private static final int LOOKUP_TIMEOUT = (int) TimeUnit.MILLISECONDS.toMillis(500);

    private final DistributedLinkedByteList.MetaData metaData;
    private final DistributedLinkedByteList.DataHolder<T> current;
    private final DistributedLinkedByteList.DataHolder<T> previous;

    private final ChunkService chunkService;
    private ByteBufferImExporter exporter;
    private ByteBuffer byteBuffer;
    private Supplier<T> m_supplier;

    //in bytes (128kb)
    private static final int MAXIMUM_STORAGE_SIZE = 128000 - Long.BYTES - Integer.BYTES;

    public DistributedLinkedByteList(final MetaData metaData, final ChunkService chunkService, final Supplier<T> p_supplier) {
        this.metaData = metaData;
        this.chunkService = chunkService;
        m_supplier = p_supplier;
        current = new DistributedLinkedByteList.DataHolder(p_supplier);
        previous = new DistributedLinkedByteList.DataHolder(p_supplier);
    }

    public void add(T[] values, short nodeId) {
        syncedWrite(() -> {
            int valuesSize = Util.sizeOfObjectArray(values);
            System.out.println("valuesSize = " + valuesSize);

            if (MAXIMUM_STORAGE_SIZE - current.sizeOfByteValues() > valuesSize) {
                fillUpCurrentBuffer(values, nodeId);

            } else {
                //create new linked entry
                current.reset();
                //TODO: need to differ if the valuesize ist just bigger MAXIMUM CAP or MAXIMUM CAP - current.size
                int restBufferSize = MAXIMUM_STORAGE_SIZE - current.sizeOfByteValues();
                int numberOfObjects = restBufferSize / m_supplier.get().sizeofObject();

                //first fillUp

                byteBuffer = ByteBuffer.allocate(valuesSize);
                exporter = new ByteBufferImExporter(byteBuffer);
                for (int i = 0; i < numberOfObjects; i++) {
                    //put entries in current values
                    T val = values[i];
                    exporter.exportObject(val);
                }
                byte[] bytesOfBuffer = byteBuffer.array();
                byte[] combined = new byte[current.values.length + bytesOfBuffer.length];
                System.arraycopy(current.values, 0, combined, 0, current.values.length);
                System.arraycopy(bytesOfBuffer, 0, combined, current.values.length, bytesOfBuffer.length);

                current.values = combined;
                current.m_count += values.length;

                chunkService.put().put(current);

                //Then create new with the rest
                byteBuffer = ByteBuffer.allocate(valuesSize);
                exporter = new ByteBufferImExporter(byteBuffer);
                for (int i = 0; i < values.length; i++) {
                    //put entries in current values
                    T val = values[i];
                    exporter.exportObject(val);
                }
                current.values = byteBuffer.array();
                current.m_count = values.length;

                current.setNext(ChunkID.INVALID_ID);
                chunkService.create().create(nodeId, current);

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

                chunkService.put().put(current);
                metaData.listSize += 1;
            }
            metaData.numberOfObjects += values.length;

            return null;
        });
    }

    private void fillUpCurrentBuffer(T[] values, final short nodeId) {
        int valuesSize = Util.sizeOfObjectArray(values);
        byteBuffer = ByteBuffer.allocate(valuesSize);
        exporter = new ByteBufferImExporter(byteBuffer);
        for (int i = 0; i < values.length; i++) {
            //put entries in current values
            T val = values[i];
            exporter.exportObject(val);
        }
        byte[] bytesOfBuffer = byteBuffer.array();
        byte[] combined = new byte[current.values.length + bytesOfBuffer.length];
        System.arraycopy(current.values, 0, combined, 0, current.values.length);
        System.arraycopy(bytesOfBuffer, 0, combined, current.values.length, bytesOfBuffer.length);

        current.values = combined;
        current.m_count += values.length;

        if (current.getID() == ChunkID.INVALID_ID) {
            chunkService.create().create(nodeId, current);
        } else {
            chunkService.resize().resize(current);
        }

        if (metaData.head == ChunkID.INVALID_ID) {
            metaData.head = current.getID();
            metaData.tail = metaData.head;
            metaData.listSize += 1;
        }

        chunkService.put().put(current);

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
                exporter = new ByteBufferImExporter(ByteBuffer.wrap(current.values));

                for (int i = 0; i < metaData.numberOfObjects; i++) {
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


    /**
     * Retrieves an existing distributed linked list using the specified name.
     */
    public static <T extends AbstractChunk> DistributedLinkedByteList<T> get(final String name, final ServiceProvider serviceProvider, final Supplier<T> valueSupplier) {
        NameserviceService nameService = serviceProvider.getService(NameserviceService.class);
        ChunkService chunkService = serviceProvider.getService(ChunkService.class);

        long chunkId = nameService.getChunkID(name, LOOKUP_TIMEOUT);
        if (chunkId == ChunkID.INVALID_ID) {
            throw new ElementNotFoundException(String.format("Chunk with nameservice id %s not found", name));
        }

        MetaData metaData = new MetaData(chunkId);
        if (!chunkService.get().get(metaData)) {
            throw new ElementNotFoundException(String.format("Chunk wit id %08X not found", chunkId));
        }

        return new DistributedLinkedByteList<>(metaData, chunkService, valueSupplier);
    }

    /**
     * Creates a new distributed linked list using the specified name if it doesn't exist yet.
     */
    public static <T extends AbstractChunk> DistributedLinkedByteList<T> create(final String name, final ServiceProvider serviceProvider, final Supplier<T> valueSupplier) {
        NameserviceService nameService = serviceProvider.getService(NameserviceService.class);
        ChunkService chunkService = serviceProvider.getService(ChunkService.class);

        if (nameService.getChunkID(name, LOOKUP_TIMEOUT) != ChunkID.INVALID_ID) {
            throw new ElementAlreadyExistsException(String.format("Chunk with nameservice id %s is already registered", name));
        }

        MetaData metaData = new MetaData();
        chunkService.createLocal().create(metaData);
        chunkService.put().put(metaData);
        nameService.register(metaData, name);
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

    private static final class DataHolder<T extends AbstractChunk> extends AbstractChunk {

        private byte[] values;
        private int m_count;
        private long next = ChunkID.INVALID_ID;

        public DataHolder(final Supplier<T> supplier) {
            T tmp = supplier.get();
            values = new byte[0];
            m_count = 0;
        }


        @Override
        public void exportObject(Exporter exporter) {
            exporter.writeByteArray(values);
            exporter.writeLong(next);
            exporter.writeInt(m_count);
        }

        @Override
        public void importObject(Importer importer) {
            values = importer.readByteArray(values);
            next = importer.readLong(next);
            m_count = importer.readInt(m_count);
        }

        @Override
        public int sizeofObject() {
            return ObjectSizeUtil.sizeofByteArray(values) + Long.BYTES + Integer.BYTES;
        }

        public int sizeOfByteValues() {
            return ObjectSizeUtil.sizeofByteArray(values);
        }

        public long getNext() {
            return next;
        }

        public void setNext(long next) {
            this.next = next;
        }

        public void reset() {
            next = ChunkID.INVALID_ID;
            setID(ChunkID.INVALID_ID);
            setState(ChunkState.UNDEFINED);
        }

    }
}
