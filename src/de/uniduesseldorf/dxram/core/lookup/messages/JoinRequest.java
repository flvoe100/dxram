package de.uniduesseldorf.dxram.core.lookup.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractRequest;
import de.uniduesseldorf.utils.Contract;

/**
 * Join Request
 * @author Kevin Beineke
 *         06.09.2012
 */
public class JoinRequest extends AbstractRequest {

	// Attributes
	private short m_newNode;
	private boolean m_nodeIsSuperpeer;

	// Constructors
	/**
	 * Creates an instance of JoinRequest
	 */
	public JoinRequest() {
		super();

		m_newNode = -1;
		m_nodeIsSuperpeer = false;
	}

	/**
	 * Creates an instance of JoinRequest
	 * @param p_destination
	 *            the destination
	 * @param p_newNode
	 *            the NodeID of the new node
	 * @param p_nodeIsSuperpeer
	 *            wether the new node is a superpeer or not
	 */
	public JoinRequest(final short p_destination, final short p_newNode, final boolean p_nodeIsSuperpeer) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_JOIN_REQUEST);

		Contract.checkNotNull(p_newNode, "new LookupNode is null");

		m_newNode = p_newNode;
		m_nodeIsSuperpeer = p_nodeIsSuperpeer;
	}

	// Getters
	/**
	 * Get new node
	 * @return the NodeID
	 */
	public final short getNewNode() {
		return m_newNode;
	}

	/**
	 * Get role of new node
	 * @return true if the new node is a superpeer, false otherwise
	 */
	public final boolean nodeIsSuperpeer() {
		return m_nodeIsSuperpeer;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putShort(m_newNode);
		p_buffer.put((byte) (m_nodeIsSuperpeer ? 1 : 0));
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_newNode = p_buffer.getShort();
		m_nodeIsSuperpeer = p_buffer.get() == 0 ? false : true;
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Short.BYTES + Byte.BYTES;
	}

}