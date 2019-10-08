package de.hhu.bsinfo.dxram.loading;

import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;

public class DxGraph {

    private String m_datasetDirectoryPath;
    private Format m_datasetFormat;
    private GraphLoadingMetaData m_metaData;
    private boolean m_fileForEveryNode;

    private short m_masterNodeID;
    private JobService m_jobService;

    private Graph m_graph;

    public DxGraph(String datasetDirectoryPath, Format p_datasetFormat, GraphLoadingMetaData p_metaData, boolean p_fileForEveryNode) {
        this.m_datasetDirectoryPath = datasetDirectoryPath;
        this.m_datasetFormat = p_datasetFormat;
        this.m_metaData = p_metaData;
        this.m_fileForEveryNode = p_fileForEveryNode;
        this.m_graph = new Graph();
        MasterSlaveComputeService a;

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
        VerticesLoadingJob job;
        for (short nodeID : m_metaData.getPeers()) {
            if(m_fileForEveryNode) {

            }
            job = new VerticesLoadingJob(m_datasetFormat.getVertexFilePath(), m_datasetFormat.getVertexLoader(),m_graph);
            if (nodeID == m_masterNodeID) {
                m_jobService.pushJob(job);
            }
            m_jobService.pushJobRemote(job, nodeID);
        }
        if (!m_jobService.waitForAllJobsToFinish()) {
            //error!
        }
    }
}
