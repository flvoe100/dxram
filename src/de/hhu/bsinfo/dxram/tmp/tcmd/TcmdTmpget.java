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

package de.hhu.bsinfo.dxram.tmp.tcmd;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;

/**
 * Get a chunk from the temporary storage
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdTmpget extends TerminalCommand {
    public TcmdTmpget() {
        super("tmpget");
    }

    @Override
    public String getHelp() {
        return "Get a chunk from the temporary storage\n" + "Usage (1): tmpget <id> <className>\n" + "Usage (2): tmpget <id> [type] [hex] [offset] [length]\n" +
            "  id: Id of the chunk stored in temporary storage\n" +
            "  className: Full name of a java class that implements DataStructure (with package path). " +
            "An instance is created, the data is stored in that instance and printed.\n " +
            "  type: Format to print the data (str, byte, short, int, long), defaults to byte\n" +
            "  hex: For some representations, print as hex instead of decimal, defaults to true\n" +
            "  offset: Offset within the chunk to start getting data from, defaults to 0\n" +
            "  length: Number of bytes of the chunk to print, defaults to size of chunk";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        int id = p_ctx.getArgInt(p_args, 0, -1);

        String className = null;
        String type = "byte";
        boolean hex = true;
        int offset = 0;
        int length = 0;

        if (p_args.length > 1) {
            if (isType(p_args[1])) {
                type = p_ctx.getArgString(p_args, 1, "byte").toLowerCase();
                hex = p_ctx.getArgBoolean(p_args, 2, true);
                offset = p_ctx.getArgInt(p_args, 3, 0);
                length = p_ctx.getArgInt(p_args, 4, 0);
            } else {
                className = p_ctx.getArgString(p_args, 1, null);
            }
        }

        if (id == -1) {
            p_ctx.printlnErr("No id specified");
            return;
        }

        TemporaryStorageService tmp = p_ctx.getService(TemporaryStorageService.class);

        if (className != null) {
            DataStructure dataStructure = newDataStructure(className, p_ctx);
            if (dataStructure == null) {
                p_ctx.printflnErr("Creating data structure of name '%s' failed", className);
                return;
            }

            dataStructure.setID(id);

            if (!tmp.get(dataStructure)) {
                p_ctx.printflnErr("Getting data structure 0x%X failed: %s", id, dataStructure.getState());
                return;
            }

            p_ctx.printfln("DataStructure %s (size %d):\n%s", className, dataStructure.sizeofObject(), dataStructure);
        } else {
            ChunkAnon chunk = new ChunkAnon(id);
            if (!tmp.getAnon(chunk)) {
                p_ctx.printflnErr("Getting chunk 0x%X failed: %s", id, chunk.getState());
                return;
            }

            if (length == 0 || length > chunk.getDataSize()) {
                length = chunk.getDataSize();
            }

            if (offset > length) {
                offset = length;
            }

            if (offset + length > chunk.getDataSize()) {
                length = chunk.getDataSize() - offset;
            }

            String str = "";
            ByteBuffer byteBuffer = ByteBuffer.wrap(chunk.getData());
            byteBuffer.position(offset);

            switch (type) {
                case "str":
                    try {
                        str = new String(chunk.getData(), offset, length, "US-ASCII");
                    } catch (final UnsupportedEncodingException e) {
                        p_ctx.printflnErr("Error encoding string: %s", e.getMessage());
                        return;
                    }

                    break;

                case "byte":
                    for (int i = 0; i < length; i += Byte.BYTES) {
                        if (hex) {
                            str += Integer.toHexString(byteBuffer.get() & 0xFF) + ' ';
                        } else {
                            str += byteBuffer.get() + " ";
                        }
                    }
                    break;

                case "short":
                    for (int i = 0; i < length; i += java.lang.Short.BYTES) {
                        if (hex) {
                            str += Integer.toHexString(byteBuffer.getShort() & 0xFFFF) + ' ';
                        } else {
                            str += byteBuffer.getShort() + " ";
                        }
                    }
                    break;

                case "int":
                    for (int i = 0; i < length; i += java.lang.Integer.BYTES) {
                        if (hex) {
                            str += Integer.toHexString(byteBuffer.getInt()) + ' ';
                        } else {
                            str += byteBuffer.getInt() + " ";
                        }
                    }
                    break;

                case "long":
                    for (int i = 0; i < length; i += java.lang.Long.BYTES) {
                        if (hex) {
                            str += Long.toHexString(byteBuffer.getLong()) + ' ';
                        } else {
                            str += byteBuffer.getLong() + " ";
                        }
                    }
                    break;

                default:
                    p_ctx.printflnErr("Unsuported data type %s", type);
                    return;
            }

            p_ctx.printfln("Chunk data of 0x%X (chunksize %d): \n%s", id, chunk.sizeofObject(), str);
        }
    }

    private static boolean isType(final String p_str) {
        switch (p_str) {
            case "str":
            case "byte":
            case "short":
            case "int":
            case "long":
                return true;
            default:
                return false;
        }
    }

    private static DataStructure newDataStructure(final String p_className, final TerminalCommandContext p_ctx) {
        Class<?> clazz;
        try {
            clazz = Class.forName(p_className);
        } catch (final ClassNotFoundException ignored) {
            p_ctx.printflnErr("Cannot find class with name %s", p_className);
            return null;
        }

        if (!DataStructure.class.isAssignableFrom(clazz)) {
            p_ctx.printflnErr("Class %s is not implementing the DataStructure interface", p_className);
            return null;
        }

        DataStructure dataStructure;
        try {
            dataStructure = (DataStructure) clazz.getConstructor().newInstance();
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            p_ctx.printflnErr("Creating instance of %s failed: %s", p_className, e.getMessage());
            return null;
        }

        return dataStructure;
    }
}
