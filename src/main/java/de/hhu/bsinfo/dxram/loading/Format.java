package de.hhu.bsinfo.dxram.loading;


import java.nio.file.Path;

public abstract class Format {
    private boolean hasVertexFile;
    private boolean hasPropertiesFile;
    private Class propertiesLoader;


    public Format(boolean hasVertexFile, boolean hasPropertiesFile, Class propertiesLoader) {
        this.hasVertexFile = hasVertexFile;
        this.hasPropertiesFile = hasPropertiesFile;
        this.propertiesLoader = propertiesLoader;

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


    abstract public Path getPropertiesFilePath();

    abstract public Path getVertexFilePath();

    abstract public Path getEdgeFilePath();

}
