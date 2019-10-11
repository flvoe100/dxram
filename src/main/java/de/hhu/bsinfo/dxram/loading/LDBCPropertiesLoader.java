package de.hhu.bsinfo.dxram.loading;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LDBCPropertiesLoader implements FileLoader {

    private final String PREFIX_NUM_OF_VERTICES = ".meta.vertices = ";
    private final String PREFIX_NUM_OF_EDGES = ".meta.edges = ";
    private final String PREFIX_IS_DIRECTED = ".meta.directed = ";

    private static final Logger LOGGER = LogManager.getFormatterLogger(LDBCPropertiesLoader.class.getSimpleName());


    @Override
    public void readFile(Path p_file, Graph p_graph) {
        try {
            Files.lines(p_file)
                    .filter(line -> line.contains(PREFIX_NUM_OF_VERTICES) || line.contains(PREFIX_NUM_OF_EDGES) || line.contains(PREFIX_IS_DIRECTED))
                    .forEach(relevantLines -> {
                        if (relevantLines.contains(PREFIX_NUM_OF_VERTICES)) {
                            p_graph.setNumberOfVertices(Integer.parseInt(relevantLines.split( PREFIX_NUM_OF_VERTICES)[1]));
                        }
                        if (relevantLines.contains(PREFIX_NUM_OF_EDGES)) {
                            p_graph.setNumberOfEdges(Integer.parseInt(relevantLines.split(PREFIX_NUM_OF_EDGES)[1]));
                        }
                        if (relevantLines.contains(PREFIX_IS_DIRECTED)) {
                            p_graph.setIsDirected(Boolean.parseBoolean(relevantLines.split(PREFIX_IS_DIRECTED)[1]));
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
