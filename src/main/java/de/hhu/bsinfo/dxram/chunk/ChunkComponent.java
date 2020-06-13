/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.Component;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxutils.dependency.Dependency;

/**
 * Component class for local chunk handling (access to local key-value memory)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
@Module.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class ChunkComponent extends Component<ChunkComponentConfig> {

    @Dependency
    private BootComponent m_boot;

    private DXMem m_memory;

    /**
     * Check if the key-value backend storage is enabled.
     *
     * @return True if enabled, false otherwise.
     */
    public boolean isStorageEnabled() {
        return m_memory != null;
    }

    /**
     * Get the local key-value memory instance
     *
     * @return DXMem instance
     */
    public DXMem getMemory() {
        if (m_memory == null) {
            throw new IllegalStateException("Cannot access local chunk storage, disabled by configuration");
        }

        return m_memory;
    }

    @Override
    protected boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager) {
        ChunkComponentConfig chunkConfig = p_config.getComponentConfig(ChunkComponent.class);

        if (chunkConfig.isChunkStorageEnabled()) {
            LOGGER.info("Allocating %d MB of native memory",
                    chunkConfig.getKeyValueStoreSize().getMB());

            m_memory = new DXMem(m_boot.getNodeId(), chunkConfig.getKeyValueStoreSize().getBytes(), chunkConfig.getSpareLIDStoreSize(),
                    chunkConfig.isChunkLockDisabled());
        } else {
            LOGGER.info("Chunk storage disabled");
            m_memory = null;
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        if (m_memory != null) {
            m_memory.shutdown();
        }

        m_memory = null;

        return true;
    }
}
