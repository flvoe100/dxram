package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;

public class Util {

    public static int sizeOfObjectArray(Object[] objects) {
        if (objects.length == 0) return 0;

        if (AbstractChunk.class.isAssignableFrom(objects[0].getClass())) {
            AbstractChunk tmp = (AbstractChunk) objects[0];
            return objects.length * tmp.sizeofObject();
        }
        return -1;
    }
}
