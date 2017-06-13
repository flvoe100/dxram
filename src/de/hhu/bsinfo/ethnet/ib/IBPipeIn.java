package de.hhu.bsinfo.ethnet.ib;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.core.AbstractFlowControl;
import de.hhu.bsinfo.ethnet.core.AbstractPipeIn;
import de.hhu.bsinfo.ethnet.core.DataReceiver;
import de.hhu.bsinfo.ethnet.core.MessageDirectory;
import de.hhu.bsinfo.ethnet.core.RequestMap;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBPipeIn extends AbstractPipeIn {

    public IBPipeIn(short p_ownNodeId, short p_destinationNodeId, AbstractFlowControl p_flowControl, MessageDirectory p_messageDirectory,
            RequestMap p_requestMap, DataReceiver p_dataReceiver) {
        super(p_ownNodeId, p_destinationNodeId, p_flowControl, p_messageDirectory, p_requestMap, p_dataReceiver);
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void returnProcessedBuffer(ByteBuffer p_buffer) {

    }
}
