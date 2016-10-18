function help() {
	return "Prints the look up tree of a specified node\n" +
			"Parameters: nid\n" +
			"  nid: Node id of the peer to print the lookup tree of";
}

function exec(nid) {

    if (nid == null) {
        dxterm.printlnErr("No nid specified");
        return;
    }

    var lookup = dxram.service("lookup");

    var respSuperpeer = lookup.getResponsibleSuperPeer(nid);

    if (respSuperpeer == -1) {
        dxterm.printlnErr("No responsible superpeer for " + dxram.nidHexStr(nid) + " found");
        return;
    }

    var tree = lookup.getLookupTreeFromSuperPeer(respSuperpeer, nid);
    dxterm.println("Lookup tree of " + dxram.nidHexStr(nid) + ":");
    dxterm.println(tree);
}