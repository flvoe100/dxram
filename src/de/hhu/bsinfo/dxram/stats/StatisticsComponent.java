
package de.hhu.bsinfo.dxram.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;

/**
 * Component to record statistics (time, call count, avarage values etc)
 * in DXRAM.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 */
public class StatisticsComponent extends AbstractDXRAMComponent {

	// configuration values
	@Expose
	private boolean m_record = false;

	// dependent components
	private LoggerComponent m_logger;

	private Map<String, Boolean> m_disabledRecorders = new HashMap<>();
	private ArrayList<StatisticsRecorder> m_recorders = new ArrayList<>();

	/**
	 * Constructor
	 */
	public StatisticsComponent() {
		super(DXRAMComponentOrder.Init.STATISTICS, DXRAMComponentOrder.Shutdown.STATISTICS);
	}

	/**
	 * Create a new recorder to record statistics of a module.
	 *
	 * @param p_class Class to create a recorder for.
	 * @return Id of the newly created recorder (handle).
	 */
	public int createRecorder(final Class<?> p_class) {
		StatisticsRecorder recorder = new StatisticsRecorder(m_recorders.size(), p_class.getSimpleName());
		m_recorders.add(recorder);

		// check blacklist
		if (m_disabledRecorders.get(p_class.getSimpleName()) != null) {
			recorder.setEnabled(false);
		}

		return recorder.getId();
	}

	/**
	 * Add a new operation to an existing recorder.
	 *
	 * @param p_id            Recorder id to add a new operation to.
	 * @param p_operationName Name of the operation to create.
	 * @return Id of the operation created (handle).
	 */
	public int createOperation(final int p_id, final String p_operationName) {
		StatisticsRecorder recorder = m_recorders.get(p_id);
		if (recorder == null) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot create operation " + p_operationName + " for recorder id " + p_id
					+ " no such recorder registered.");
			// #endif /* LOGGER >= ERROR */
			return StatisticsRecorder.Operation.INVALID_ID;
		}

		return recorder.createOperation(p_operationName);
	}

	/**
	 * Call this when/before you start/enter the call/operation you want
	 * to record.
	 *
	 * @param p_recorderId  Id of the recorder to record this call on.
	 * @param p_operationId Id of the operation to record.
	 */
	public void enter(final int p_recorderId, final int p_operationId) {
		if (!m_record) {
			return;
		}

		StatisticsRecorder recorder = m_recorders.get(p_recorderId);
		if (recorder == null) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot enter operation " + p_operationId + " for recorder id " + p_recorderId
					+ " no such recorder registered.");
			// #endif /* LOGGER >= ERROR */
			return;
		}

		StatisticsRecorder.Operation operation = recorder.getOperation(p_operationId);
		if (operation == null) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot enter operation " + p_operationId + " for recorder id " + p_recorderId
					+ " no such operation registered.");
			// #endif /* LOGGER >= ERROR */
			return;
		}

		operation.enter();
	}

	/**
	 * Call this when/before you start/enter the call/operation you want
	 * to record.
	 *
	 * @param p_recorderId  Id of the recorder to record this call on.
	 * @param p_operationId Id of the operation to record.
	 * @param p_val         Additional value to be added to the long counter.
	 */
	public void enter(final int p_recorderId, final int p_operationId, final long p_val) {
		if (!m_record) {
			return;
		}

		StatisticsRecorder recorder = m_recorders.get(p_recorderId);
		if (recorder == null) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot enter operation " + p_operationId + " for recorder id " + p_recorderId
					+ " no such recorder registered.");
			// #endif /* LOGGER >= ERROR */
			return;
		}

		StatisticsRecorder.Operation operation = recorder.getOperation(p_operationId);
		if (operation == null) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot enter operation " + p_operationId + " for recorder id " + p_recorderId
					+ " no such operation registered.");
			// #endif /* LOGGER >= ERROR */
			return;
		}

		operation.enter(p_val);
	}

	/**
	 * Call this when/before you start/enter the call/operation you want
	 * to record.
	 *
	 * @param p_recorderId  Id of the recorder to record this call on.
	 * @param p_operationId Id of the operation to record.
	 * @param p_val         Additional value to be added to the double counter.
	 */
	public void enter(final int p_recorderId, final int p_operationId, final double p_val) {
		if (!m_record) {
			return;
		}

		StatisticsRecorder recorder = m_recorders.get(p_recorderId);
		if (recorder == null) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot enter operation " + p_operationId + " for recorder id " + p_recorderId
					+ " no such recorder registered.");
			// #endif /* LOGGER >= ERROR */
			return;
		}

		StatisticsRecorder.Operation operation = recorder.getOperation(p_operationId);
		if (operation == null) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot enter operation " + p_operationId + " for recorder id " + p_recorderId
					+ " no such operation registered.");
			// #endif /* LOGGER >= ERROR */
			return;
		}

		operation.enter(p_val);
	}

	/**
	 * Call this when/after you ended/left the call/operation.
	 *
	 * @param p_recorderId  Id of the recorder to record this call on.
	 * @param p_operationId Id of the operation to record.
	 */
	public void leave(final int p_recorderId, final int p_operationId) {
		if (!m_record) {
			return;
		}

		StatisticsRecorder recorder = m_recorders.get(p_recorderId);
		if (recorder == null) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot leave operation " + p_operationId + " for recorder id " + p_recorderId
					+ " no such recorder registered.");
			// #endif /* LOGGER >= ERROR */
			return;
		}

		StatisticsRecorder.Operation operation = recorder.getOperation(p_operationId);
		if (operation == null) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot leave operation " + p_operationId + " for recorder id " + p_recorderId
					+ " no such operation registered.");
			// #endif /* LOGGER >= ERROR */
			return;
		}

		operation.leave();
	}

	/**
	 * Get a list of all registered/created recorders.
	 *
	 * @return List of StatisticsRecorders.
	 */
	public ArrayList<StatisticsRecorder> getRecorders() {
		return m_recorders;
	}

	/**
	 * Get a specific recorder.
	 *
	 * @param p_class Class this recorder was created for.
	 * @return StatisticsRecorder if one was created for that class or null.
	 */
	public StatisticsRecorder getRecorder(final Class<?> p_class) {
		for (StatisticsRecorder recorder : m_recorders) {
			if (p_class.getSimpleName().equals(recorder.getName())) {
				return recorder;
			}
		}

		return null;
	}

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_logger = p_componentAccessor.getComponent(LoggerComponent.class);
    }

	@Override
	protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		// #if LOGGER >= INFO
		m_logger.info(getClass(), "Recording of statistics enabled (global override): " + m_record);
		// #endif /* LOGGER >= INFO */

		//		// read further entries, which can disable single categories (optional)
		//		Map<Integer, String> catDisabled = p_settings.getValues("/CategoryDisabled", String.class);
		//		if (catDisabled != null) {
		//			for (Entry<Integer, String> entry : catDisabled.entrySet()) {
		//				m_disabledRecorders.put(entry.getValue(), true);
		//
		//				// #if LOGGER >= DEBUG
		//				m_logger.debug(getClass(), "Recorder " + entry.getValue() + " disabled.");
		//				// #endif /* LOGGER >= DEBUG */
		//			}
		//		}

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		return true;
	}

}
