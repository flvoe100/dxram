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

package de.hhu.bsinfo.dxram.chunk.tcmd;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Create a chunk on a remote node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdChunkcreate extends TerminalCommand {
    public TcmdChunkcreate() {
        super("chunkcreate");
    }

    @Override
    public String getHelp() {
        return "Create a chunk on a remote node\n" + "Usage: chunkcreate <size> <nid>\n" + "  size: Size of the chunk to create\n" +
            "  nid: Node id of the peer to create the chunk on";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        int size = p_ctx.getArgInt(p_args, 0, -1);
        short nid = p_ctx.getArgNodeId(p_args, 1, NodeID.INVALID_ID);

        if (size == -1) {
            p_ctx.printlnErr("No size specified");
            return;
        }

        if (nid == NodeID.INVALID_ID) {
            p_ctx.printlnErr("No nid specified");
            return;
        }

        ChunkService chunk = p_ctx.getService(ChunkService.class);

        long[] chunkIDs = chunk.createRemote(nid, size);

        p_ctx.printfln("Created chunk of size %d: 0x%X", size, chunkIDs[0]);
    }
}