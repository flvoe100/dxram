package de.hhu.bsinfo.dxram.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Component to run applications locally on the DXRAM instance with access to all exposed services
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public class ApplicationComponent extends AbstractDXRAMComponent<ApplicationComponentConfig> {
    private List<Class<? extends AbstractApplication>> m_applicationClasses = new ArrayList<>();

    /**
     * Constructor
     */
    public ApplicationComponent() {
        super(DXRAMComponentOrder.Init.APPLICATION, DXRAMComponentOrder.Shutdown.APPLICATION, ApplicationComponentConfig.class);
    }

    /**
     * Get the path where all application jars are located
     *
     * @return Path with application jars
     */
    String getApplicationPath() {
        return getConfig().getApplicationPath();
    }

    /**
     * Get the loaded application classes
     *
     * @return Loaded classes implementing the application interface
     */
    List<Class<? extends AbstractApplication>> getApplicationClasses() {
        return m_applicationClasses;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {

    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {
        // #if LOGGER >= INFO
        LOGGER.info("Loading application %s", getConfig().getApplicationPath());
        // #endif /* LOGGER >= INFO */

        File dir = new File(getConfig().getApplicationPath());

        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();

            if (files != null) {
                for (File file : files) {
                    m_applicationClasses.addAll(getApplicationClasses(file));
                }
            }
        } else {
            // #if LOGGER >= WARN
            LOGGER.warn("Can't load applications from %s, no such directory", getConfig().getApplicationPath());
            // #endif /* LOGGER >= WARN */
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_applicationClasses.clear();

        return false;
    }

    /**
     * Get all classes which extend the AbstractApplication class from a far file
     *
     * @param p_jar
     *         Jar file to search for classes to load
     * @return List of loaded classes
     */
    private List<Class<? extends AbstractApplication>> getApplicationClasses(final File p_jar) {
        List<Class<? extends AbstractApplication>> classes = new ArrayList<>();

        ClassLoader classLoader = getClass().getClassLoader();
        URLClassLoader ucl;

        try {
            ucl = new URLClassLoader(new URL[] {p_jar.toURI().toURL()}, classLoader);
        } catch (final MalformedURLException e) {
            // #if LOGGER >= ERROR
            LOGGER.error(e);
            // #endif /* LOGGER >= ERROR */

            return classes;
        }

        JarInputStream jarFile;
        try {
            jarFile = new JarInputStream(new FileInputStream(p_jar));
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Opening jar %s failed: %s", p_jar.getAbsolutePath(), e.getMessage());
            // #endif /* LOGGER >= ERROR */

            return classes;
        }

        while (true) {
            JarEntry jarEntry = null;
            try {
                jarEntry = jarFile.getNextJarEntry();
            } catch (final IOException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Getting next jar entry from %s failed: %s", p_jar.getAbsolutePath(), e.getMessage());
                // #endif /* LOGGER >= ERROR */
            }

            if (jarEntry == null) {
                break;
            }

            if (jarEntry.getName().endsWith(".class")) {
                String classname = jarEntry.getName().replaceAll("/", "\\.");
                classname = classname.substring(0, classname.length() - 6);

                try {
                    Class<?> clazz = Class.forName(classname, true, ucl);

                    if (AbstractApplication.class.isAssignableFrom(clazz)) {
                        // #if LOGGER >= INFO
                        LOGGER.info("Found application %s", clazz.getName());
                        // #endif /* LOGGER >= INFO */

                        // check for default constructor
                        clazz.getConstructor();

                        classes.add((Class<? extends AbstractApplication>) clazz);
                    }
                } catch (final ClassNotFoundException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Could not find class %s in jar %s", classname, p_jar.getAbsolutePath());
                    // #endif /* LOGGER >= ERROR */
                } catch (final NoSuchMethodException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Could not load class %s in jar %s, missing default constructor", classname, p_jar.getAbsolutePath());
                    // #endif /* LOGGER >= ERROR */
                }
            }
        }

        return classes;
    }
}
