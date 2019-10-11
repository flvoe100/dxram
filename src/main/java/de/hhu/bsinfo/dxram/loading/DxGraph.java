package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.engine.ServiceProvider;

import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.TaskScriptState;
import de.hhu.bsinfo.dxram.ms.script.TaskScript;

public class DxGraph {

    private String m_datasetDirectoryPath;
    private Format m_datasetFormat;
    private GraphLoadingMetaData m_metaData;
    private boolean m_fileForEveryNode;

    private short m_masterNodeID;
    private MasterSlaveComputeService m_masterSlaveService;

    private Graph m_graph;

    public DxGraph(ServiceProvider p_context, String datasetDirectoryPath, Format p_datasetFormat, GraphLoadingMetaData p_metaData, boolean p_fileForEveryNode) {
        this.m_datasetDirectoryPath = datasetDirectoryPath;
        this.m_datasetFormat = p_datasetFormat;
        this.m_metaData = p_metaData;
        this.m_fileForEveryNode = p_fileForEveryNode;
        this.m_graph = new Graph();
        this.m_masterSlaveService = p_context.getService(MasterSlaveComputeService.class);
    }

    public void loadGraph() {
        if (m_datasetFormat.hasPropertiesFile()) {
            this.loadProperties();
        }
        if(m_datasetFormat.hasVertexFile()) {
            this.loadVertices();
        }
    }

    private void loadProperties() {
        this.m_datasetFormat.getPropertiesLoader().readFile(this.m_datasetFormat.getPropertiesFilePath(), m_graph);
    }

    private void loadVertices() {
        System.out.println("m_graphV = " + m_graph.getNumberOfVertices());

        VerticesLoadingTask verticesLoadingTask = new VerticesLoadingTask(this.m_datasetFormat.getVertexFilePath().toString(), m_datasetFormat.getVertexLoader().getClass().getName(), m_graph);
        TaskScript taskScript = new TaskScript(verticesLoadingTask);
        TaskScriptState state =m_masterSlaveService.submitTaskScript(taskScript);

        while (!state.hasTaskCompleted()) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }


    }
}
