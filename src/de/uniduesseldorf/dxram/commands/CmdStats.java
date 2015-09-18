
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

// AppID später optional abfragen

/**
 * Get info about the log module of a peer
 * @author Kevin Beineke 10.09.2015
 */
public class CmdStats extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdStats() {}

	@Override
	public String getName() {
		return "stats";
	}

	@Override
	public String getUsageMessage() {
		return "stats NID";
	}

	@Override
	public String getHelpMessage() {
		return "Get the current statistics of a node.\n";
	}

	@Override
	public String getSyntax() {
		return "stats ANID";
	}

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		boolean ret = false;
		String res = null;
		String[] arguments;
		short nodeID;

		try {
			arguments = p_command.split(" ");

			// get NID to send command to
			if (arguments.length == 2) {
				nodeID = CmdUtils.getNIDfromTuple(arguments[1]);

				if (!CmdUtils.checkNID(Short.toString(nodeID)).equals("unknown")) {
					res = Core.executeChunkCommand(nodeID, p_command, true);

					// process result of remote call
					if (!res.contains("error")) {
						System.out.println(res);
					}
				}
			}
		} catch (final DXRAMException e) {
			System.out.println("  error: Core.execute failed");
			ret = false;
		}

		return ret;
	}
}