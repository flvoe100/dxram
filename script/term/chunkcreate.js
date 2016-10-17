function help() {
	return "Create a chunk on a remote node\n" +
			"Parameters: size nid\n" +
			"  size: Size of the chunk to create\n" +
			"  nid: Node id of the peer to create the chunk on"
}

function exec(size, nid) {

	if (size == null) {
		print("No size specified")
		return
	}
	
	if (nid == null) {
		print("No nid specified")
		return
	}

	var chunk = dxram.service("chunk")

	var chunkIDs = chunk.createRemote(nid, size)

	if (chunkIDs != null) {
	    print("Created chunk of size " + size + ": " + dxram.cidhexstr(chunkIDs[0]))
	} else {
        print("Creating chunk failed.")
	}
}
