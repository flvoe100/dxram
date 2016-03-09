package de.hhu.bsinfo.dxram.migrate.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a migration request.
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class MigrationResponse extends AbstractResponse {

	private long[] m_failedMigrationIDs = null;
	
	/**
	 * Creates an instance of DataResponse.
	 * This constructor is used when receiving this message.
	 */
	public MigrationResponse() {
		super();
	}

	/**
	 * Creates an instance of DataResponse.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            the request
	 * @param p_failedMIgrationIDs List of failed migration IDs.
	 */
	public MigrationResponse(final MigrationRequest p_request, final long[] p_failedMigrationIDs) {
		super(p_request, MigrationMessages.SUBTYPE_MIGRATION_RESPONSE);
		
		m_failedMigrationIDs = p_failedMigrationIDs;
	}
	
	/**
	 * Get the IDs of the chunks, which failed to migrate.
	 * @return Array with IDs that failed to migrate or null if migration was successful.
	 */
	public long[] getFailedMigrationIDs()
	{
		return m_failedMigrationIDs;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_failedMigrationIDs.length);
		
		for (long failedID : m_failedMigrationIDs) {
			p_buffer.putLong(failedID);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_failedMigrationIDs = new long[p_buffer.getInt()];
		
		for (int i = 0; i < m_failedMigrationIDs.length; i++) {
			m_failedMigrationIDs[i] = p_buffer.getLong();
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Integer.BYTES + m_failedMigrationIDs.length * Long.BYTES;
	}
}