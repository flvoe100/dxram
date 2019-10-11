package de.hhu.bsinfo.dxram.loading;


import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.nio.file.Path;

public abstract class Format extends AbstractChunk {
    private boolean hasVertexFile;
    private boolean hasPropertiesFile;
    private Class propertiesLoader;
    private Class verticesJobLoader;
    private Class edgeLoader;

    public Format(boolean hasVertexFile, boolean hasPropertiesFile, Class propertiesLoader, Class vertexLoader, Class edgeLoader) {
        this.hasVertexFile = hasVertexFile;
        this.hasPropertiesFile = hasPropertiesFile;
        this.propertiesLoader = propertiesLoader;
        this.verticesJobLoader = vertexLoader;
        this.edgeLoader = edgeLoader;

    }

    public boolean hasVertexFile() {
        return hasVertexFile;
    }

    public boolean hasPropertiesFile() {
        return hasPropertiesFile;
    }

    public FileLoader getPropertiesLoader() {
        try {
            return (FileLoader) this.propertiesLoader.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public FileLoader getVertexLoader() {
        try {
            return (FileLoader) this.verticesJobLoader.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public FileLoader getEdgeLoader() {
        try {
            return (FileLoader) this.edgeLoader.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    abstract public Path getPropertiesFilePath();

    abstract public Path getVertexFilePath();

    abstract public Path getEdgeFilePath();

    @Override
    public void importObject(Importer p_importer) {
    }

    @Override
    public void exportObject(Exporter p_exporter) {

    }

    @Override
    public int sizeofObject() {
        return 0;
    }
}
