package de.hhu.bsinfo.dxram.loading;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.nio.file.Paths;

public class VerticesLoadingTask implements Task {
    @Expose
   private String vertexFilePath;


    @Expose
    private String loaderClassName;

    @Expose
    private Graph graph;


    public VerticesLoadingTask() {}

    public VerticesLoadingTask(String vertexFilePath, String loaderClassName, Graph graph) {
        this.vertexFilePath = vertexFilePath;
        this.loaderClassName = loaderClassName;
        this.graph = graph;
    }

    @Override
    public int execute(TaskContext p_ctx) {
        System.out.println("YEHOOOO");
        System.out.println(graph.getNumberOfVertices());
        System.out.println(graph.isM_isDirected());
        System.out.println(graph.getM_numberOfEdges());
        LDBCVertexLoader loader = null;
        try {
            loader = (LDBCVertexLoader) Class.forName(loaderClassName).newInstance();
            loader.setLocalService(p_ctx.getDXRAMServiceAccessor().getService(ChunkLocalService.class));
            loader.setChunkService(p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class));
            loader.readFile(Paths.get(vertexFilePath), graph);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


        loader.readFile(Paths.get(vertexFilePath), null);


        return 0;
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
       p_exporter.writeString(vertexFilePath);
       p_exporter.writeString(loaderClassName);
       p_exporter.exportObject(graph);

    }

    @Override
    public void importObject(Importer p_importer) {
       vertexFilePath = p_importer.readString(vertexFilePath);
        loaderClassName = p_importer.readString(loaderClassName);
        if(graph == null){
          graph = new Graph();
        }
        graph.importObject(p_importer);

    }

    @Override
    public int sizeofObject() {
       return ObjectSizeUtil.sizeofString(vertexFilePath) + ObjectSizeUtil.sizeofString(loaderClassName) + graph.sizeofObject();

    }
}
