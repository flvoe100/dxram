package de.hhu.bsinfo.dxram.loading;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LDBCPropertiesLoader implements FileLoader {

    private final String PREFIX_NUM_OF_VERTICES = ".meta.vertices = ";
    private final String PREFIX_NUM_OF_EDGES = ".meta.edges = ";
    private final String PREFIX_IS_DIRECTED = ".meta.vertices = ";

    @Override
    public void readFile(Path p_file, Graph p_graph) {
        try {
            Files.lines(p_file)
                    .filter(line -> line.contains(PREFIX_NUM_OF_VERTICES) || line.contains(PREFIX_NUM_OF_EDGES) || line.contains(PREFIX_IS_DIRECTED))
                    .forEach(relevantLines -> {
                        if (relevantLines.contains(PREFIX_NUM_OF_VERTICES)) {
                            p_graph.setM_numberOfVertices(Integer.parseInt(relevantLines.split( PREFIX_NUM_OF_VERTICES)[1]));
                        }
                        if (relevantLines.contains(PREFIX_NUM_OF_EDGES)) {
                            p_graph.setM_numberOfEdges(Integer.parseInt(relevantLines.split(PREFIX_NUM_OF_VERTICES)[1]));
                        }
                        if (relevantLines.contains(PREFIX_IS_DIRECTED)) {
                            p_graph.setM_isDirected(Boolean.parseBoolean(relevantLines.split(PREFIX_IS_DIRECTED)[1]));
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
