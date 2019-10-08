package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;

public abstract class Vertex extends AbstractChunk {

    public long getVertexID() {
        return ChunkID.getLocalID(this.getID());
    }
}
