package de.hhu.bsinfo.dxram.stats;

import java.util.Collection;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Service for internal statistics
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.04.2017
 */
public class StatisticsService extends AbstractDXRAMService {
    /**
     * Constructor
     */
    public StatisticsService() {
        super("stats", true, true);
    }

    /**
     * Returns the recorders
     *
     * @return the recorders
     */
    public Collection<StatisticsRecorder> getRecorders() {
        return StatisticsRecorderManager.getRecorders();
    }

    /**
     * Returns the recorder
     *
     * @param p_name
     *         Get the recorder by its name
     * @return the recorder
     */
    public StatisticsRecorder getRecorder(final String p_name) {
        return StatisticsRecorderManager.getRecorder(p_name);
    }

    @Override
    protected boolean supportedBySuperpeer() {
        return true;
    }

    @Override
    protected boolean supportedByPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {

    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}
