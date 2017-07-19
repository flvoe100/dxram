/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.net.core;

import de.hhu.bsinfo.utils.serialization.Exporter;

/**
 * Abstraction of an Exporter for network messages.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 05.07.2017
 */
public abstract class AbstractMessageExporter implements Exporter {

    /**
     * Constructor
     */
    protected AbstractMessageExporter() {
    }

    /**
     * Get the number of serialized bytes.
     *
     * @return number of written bytes
     */
    protected abstract int getNumberOfWrittenBytes();

    /**
     * Set buffer to write into.
     *
     * @param p_buffer
     *         the byte array
     */
    protected abstract void setBuffer(byte[] p_buffer);

    /**
     * Set buffer offset
     *
     * @param p_position
     *         the offset
     */
    protected abstract void setPosition(int p_position);

}
