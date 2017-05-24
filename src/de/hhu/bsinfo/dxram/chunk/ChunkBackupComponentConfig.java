package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

/**
 * Config for the ChunkBackupComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class ChunkBackupComponentConfig extends DXRAMComponentConfig {
    /**
     * Constructor
     */
    public ChunkBackupComponentConfig() {
        super(ChunkBackupComponent.class, false, true);
    }
}
