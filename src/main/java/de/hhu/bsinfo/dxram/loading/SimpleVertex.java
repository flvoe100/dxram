package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class SimpleVertex extends Vertex {
    private long extID;

    public SimpleVertex(){
        super();
    }

    public SimpleVertex(long extID) {
        this.extID = extID;
    }

    public long getExtID() {
        return extID;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(extID);
    }

    @Override
    public void importObject(Importer p_importer) {
        extID = p_importer.readLong(extID);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES;
    }
}
