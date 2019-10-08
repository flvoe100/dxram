package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.job.Job;

import java.nio.file.Path;

public class VerticesLoadingJob extends Job {

    private Path m_vertexFilePath;
    private FileLoader m_fileLoader;
    private Graph m_graph;

    public VerticesLoadingJob(Path p_vertexFilePath, FileLoader p_fileLoader, Graph p_graph) {
        m_vertexFilePath = p_vertexFilePath;
        m_fileLoader = p_fileLoader;
        m_graph = p_graph;
    }

    @Override
    public void execute() {
        m_fileLoader.readFile(m_vertexFilePath, m_graph);
    }
}
