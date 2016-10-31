
package de.hhu.bsinfo.dxcompute.job.messages;

import de.hhu.bsinfo.dxcompute.DXComputeMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request the status of a the job service from a remote node.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 10.02.2016
 */
public class StatusRequest extends AbstractRequest {
    /**
     * Creates an instance of StatusRequest.
     * This constructor is used when receiving this message.
     */
    public StatusRequest() {
        super();
    }

    /**
     * Creates an instance of StatusRequest.
     * This constructor is used when sending this message.
     * @param p_destination
     *            the destination node id.
     */
    public StatusRequest(final short p_destination) {
        super(p_destination, DXComputeMessageTypes.JOB_MESSAGES_TYPE, JobMessages.SUBTYPE_STATUS_REQUEST);
    }
}
