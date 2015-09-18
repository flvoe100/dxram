
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

// AppID später optional abfragen

/**
 * Get info about the log module of a peer
 * @author Kevin Beineke 10.09.2015
 */
public class CmdLogInfo extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdLogInfo() {}

	@Override
	public String getName() {
		return "loginfo";
	}

	@Override
	public String getUsageMessage() {
		return "loginfo NID";
	}

	@Override
	public String getHelpMessage() {
		return "Get the current log utilization of a peer.\n";
	}

	@Override
	public String getSyntax() {
		return "loginfo PNID";
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

				if (CmdUtils.checkNID(Short.toString(nodeID)).equals("peer")) {
					res = Core.executeLogCommand(nodeID, p_command);

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