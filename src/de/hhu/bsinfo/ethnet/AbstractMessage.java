
package de.hhu.bsinfo.ethnet;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents a network message
 *
 * @author Florian Klein 09.03.2012
 * @author Marc Ewert 18.09.2014
 */
public abstract class AbstractMessage {

	// Constants
	private static final int INVALID_MESSAGE_ID = -1;
	private static final byte DEFAULT_TYPE = 0;
	private static final byte DEFAULT_SUBTYPE = 0;
	private static final byte DEFAULT_STATUS_CODE = 0;
	protected static final boolean DEFAULT_EXCLUSIVITY_VALUE = false;

	/*- Header size:
	 *  messageID + type + subtype + exclusivity + statusCode + payloadSize
	 *  3b        + 1b   + 1b      + 1b          + 1b         + 4b           = 11 bytes
	 */
	protected static final byte HEADER_SIZE = 11;
	protected static final byte PAYLOAD_SIZE_LENGTH = 4;

	// Attributes
	// (!) MessageID occupies only 3 byte in message header
	private int m_messageID;
	private short m_source;
	private short m_destination;
	private byte m_type;
	private byte m_subtype;
	// (!) Exclusivity is written as a byte (0 -> false, 1 -> true)
	private boolean m_exclusivity;
	// status code for all messages to indicate success, errors etc.
	private byte m_statusCode;

	private static int m_nextMessageID = 1;
	private static ReentrantLock m_lock = new ReentrantLock(false);

	// Constructors

	/**
	 * Creates an instance of Message
	 */
	protected AbstractMessage() {
		m_messageID = INVALID_MESSAGE_ID;
		m_source = NodeID.INVALID_ID;
		m_destination = NodeID.INVALID_ID;
		m_type = DEFAULT_TYPE;
		m_subtype = DEFAULT_SUBTYPE;

		m_exclusivity = DEFAULT_EXCLUSIVITY_VALUE;

		m_statusCode = 0;
	}

	/**
	 * Creates an instance of Message
	 *
	 * @param p_destination the destination
	 * @param p_type        the message type
	 * @param p_subtype     the message subtype
	 */
	public AbstractMessage(final short p_destination, final byte p_type, final byte p_subtype) {
		this(getNextMessageID(), p_destination, p_type, p_subtype, DEFAULT_EXCLUSIVITY_VALUE, DEFAULT_STATUS_CODE);
	}

	/**
	 * Creates an instance of Message
	 *
	 * @param p_destination the destination
	 * @param p_type        the message type
	 * @param p_subtype     the message subtype
	 * @param p_exclusivity whether this message type allows parallel execution
	 */
	public AbstractMessage(final short p_destination, final byte p_type, final byte p_subtype,
			final boolean p_exclusivity) {
		this(getNextMessageID(), p_destination, p_type, p_subtype, p_exclusivity, DEFAULT_STATUS_CODE);
	}

	/**
	 * Creates an instance of Message
	 *
	 * @param p_messageID   the messageID
	 * @param p_destination the destination
	 * @param p_type        the message type
	 * @param p_subtype     the message subtype
	 */
	protected AbstractMessage(final int p_messageID, final short p_destination, final byte p_type,
			final byte p_subtype) {
		this(p_messageID, p_destination, p_type, p_subtype, DEFAULT_EXCLUSIVITY_VALUE, DEFAULT_STATUS_CODE);
	}

	/**
	 * Creates an instance of Message
	 *
	 * @param p_messageID   the messageID
	 * @param p_destination the destination
	 * @param p_type        the message type
	 * @param p_subtype     the message subtype
	 * @param p_exclusivity whether this is an exclusive message or not
	 * @param p_statusCode  the status code
	 */
	private AbstractMessage(final int p_messageID, final short p_destination, final byte p_type, final byte p_subtype,
			final boolean p_exclusivity,
			final byte p_statusCode) {
		assert p_destination != NodeID.INVALID_ID;

		m_messageID = p_messageID;
		m_source = -1;
		m_destination = p_destination;
		m_type = p_type;
		m_subtype = p_subtype;

		m_exclusivity = p_exclusivity;
		m_statusCode = p_statusCode;
	}

	// Getters

	/**
	 * Get the messageID
	 *
	 * @return the messageID
	 */
	public final int getMessageID() {
		return m_messageID;
	}

	/**
	 * Get the source
	 *
	 * @return the source
	 */
	public final short getSource() {
		return m_source;
	}

	/**
	 * Get the destination
	 *
	 * @return the destination
	 */
	public final short getDestination() {
		return m_destination;
	}

	/**
	 * Get the message type
	 *
	 * @return the message type
	 */
	public final byte getType() {
		return m_type;
	}

	/**
	 * Get the message subtype
	 *
	 * @return the message subtype
	 */
	public final byte getSubtype() {
		return m_subtype;
	}

	/**
	 * Get the status code (definable error, success,...)
	 *
	 * @return Status code.
	 */
	public final byte getStatusCode() {
		return m_statusCode;
	}

	/**
	 * Returns whether this message type allows parallel execution
	 *
	 * @return the exclusivity
	 */
	public final boolean isExclusive() {
		return m_exclusivity;
	}

	// Setters

	/**
	 * Set the status code (definable error, success,...)
	 *
	 * @param p_statusCode the status code
	 */
	public final void setStatusCode(final byte p_statusCode) {
		m_statusCode = p_statusCode;
	}

	/**
	 * Sets source of the message
	 *
	 * @param p_source the source node ID
	 */
	final void setSource(final short p_source) {
		m_source = p_source;
	}

	/**
	 * Sets destination of the message
	 *
	 * @param p_destination the destination node ID
	 */
	final void setDestination(final short p_destination) {
		m_destination = p_destination;
	}

	// Methods

	/**
	 * Reads the message payload from the byte buffer
	 *
	 * @param p_buffer the byte buffer
	 */
	protected void readPayload(final ByteBuffer p_buffer) {
	}

	/**
	 * Writes the message payload into the buffer
	 *
	 * @param p_buffer the buffer
	 * @throws BufferOverflowException if message buffer is too small
	 */
	protected void writePayload(final ByteBuffer p_buffer) {
	}

	/**
	 * Get the total number of bytes the payload requires to create a buffer.
	 *
	 * @return Number of bytes of the payload
	 */
	protected int getPayloadLength() {
		return 0;
	}

	/**
	 * Get a ByteBuffer with the Message as content
	 *
	 * @return a ByteBuffer with the Message as content
	 * @throws NetworkException if message buffer is too small
	 */
	protected final ByteBuffer getBuffer() throws NetworkException {
		int payloadSize;
		ByteBuffer buffer;

		payloadSize = getPayloadLength();
		buffer = ByteBuffer.allocate(HEADER_SIZE + payloadSize);
		buffer = fillBuffer(buffer, payloadSize);
		buffer.flip();

		return buffer;
	}

	/**
	 * Fills a given ByteBuffer with the message
	 *
	 * @param p_buffer      a given ByteBuffer
	 * @param p_payloadSize the payload size
	 * @return filled ByteBuffer
	 * @throws NetworkException if message buffer is too small
	 */
	private ByteBuffer fillBuffer(final ByteBuffer p_buffer, final int p_payloadSize) throws NetworkException {
		try {
			// Put 3 byte message ID
			p_buffer.put((byte) (m_messageID >>> 16));
			p_buffer.put((byte) (m_messageID >>> 8));
			p_buffer.put((byte) m_messageID);

			p_buffer.put(m_type);
			p_buffer.put(m_subtype);
			if (m_exclusivity) {
				p_buffer.put((byte) 1);
			} else {
				p_buffer.put((byte) 0);
			}
			p_buffer.put(m_statusCode);
			p_buffer.putInt(p_payloadSize);

			writePayload(p_buffer);
		} catch (final BufferOverflowException e) {
			throw new NetworkException("Could not create message " + this + ", because message buffer is too small", e);
		}

		int pos = p_buffer.position();
		int payloadSize = getPayloadLength() + HEADER_SIZE;
		if (pos < payloadSize) {
			throw new NetworkException("Did not create message " + this
					+ ", because message contents are smaller than expected payload size: " + pos + " < "
					+ payloadSize);
		}

		return p_buffer;
	}

	/**
	 * Get next free messageID
	 *
	 * @return next free messageID
	 */
	private static int getNextMessageID() {
		int ret;

		m_lock.lock();
		ret = m_nextMessageID++;
		m_lock.unlock();

		return ret;
	}

	/**
	 * Executed before a Message is send (not forwarded)
	 */
	protected void beforeSend() {
	}

	/**
	 * Executed after a Message is send (not forwarded)
	 */
	protected void afterSend() {
	}

	/**
	 * Creates a Message from the given incoming byte buffer
	 *
	 * @param p_buffer           the byte buffer
	 * @param p_messageDirectory the message directory
	 * @return the created Message
	 * @throws NetworkException if the message header could not be created
	 */
	protected static AbstractMessage createMessageHeader(final ByteBuffer p_buffer,
			final MessageDirectory p_messageDirectory) throws NetworkException {
		AbstractMessage ret = null;
		int messageID;
		byte type;
		byte subtype;
		boolean exclusivity;
		byte statusCode;

		assert p_buffer != null;

		// The message header does not contain the payload size
		if (p_buffer.remaining() < HEADER_SIZE - PAYLOAD_SIZE_LENGTH) {
			throw new NetworkException("Incomplete header");
		}

		messageID = ((p_buffer.get() & 0xFF) << 16) + ((p_buffer.get() & 0xFF) << 8) + (p_buffer.get() & 0xFF);
		type = p_buffer.get();
		subtype = p_buffer.get();
		exclusivity = p_buffer.get() == 1;
		statusCode = p_buffer.get();

		try {
			ret = p_messageDirectory.getInstance(type, subtype);
		} catch (final Exception e) {
			throw new NetworkException("Unable to create message of type " + type + ", subtype " + subtype
					+ ". Type is missing in message directory", e);
		}

		ret.m_messageID = messageID;
		ret.m_type = type;
		ret.m_subtype = subtype;
		ret.m_exclusivity = exclusivity;
		ret.m_statusCode = statusCode;

		return ret;
	}

	@Override
	public final String toString() {
		if (m_source != -1) {
			return getClass().getSimpleName() + "[" + m_messageID + ", " + NodeID.toHexString(m_source) + ", "
					+ NodeID.toHexString(m_destination) + "]";
		} else {
			return getClass().getSimpleName() + "[" + m_messageID + ", " + NodeID.toHexString(m_destination) + "]";
		}
	}

}