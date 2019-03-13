/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.engine;

import lombok.Data;
import lombok.experimental.Accessors;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.dependency.DependencyGraph;

/**
 * Manager for modules (components and/or services) in DXRAM. All modules used in DXRAM must be registered here to
 * create default configurations and instances of them.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.11.2018
 */
class ModuleManager {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ModuleManager.class);

    private Map<String, ModuleContainer> m_modules = new HashMap<>();

    /**
     * Register a module
     *
     * @param p_class
     *         Component class to register
     * @param p_configClass
     *         Configuration class to associate with the specified module
     */
    void register(final Class<? extends Module> p_class,
            final Class<? extends ModuleConfig> p_configClass) {
        m_modules.put(p_class.getSimpleName(), new ModuleContainer(p_class.getSimpleName(), p_class, p_configClass));
    }

    /**
     * Create default configurations based on the currently registered modules
     *
     * @return Map with default configurations
     */
    Map<String, ModuleConfig> createDefaultConfigs() {
        Map<String, ModuleConfig> defaultConfigs = new HashMap<>();

        for (ModuleContainer moduleContainer : m_modules.values()) {
            defaultConfigs.put(moduleContainer.getName(),
                    moduleContainer.newDefaultConfigInstance(moduleContainer.getName()));
        }

        return defaultConfigs;
    }

    /**
     * Initialize the manager
     *
     * @param p_currentInstanceNodeRole
     *         The node role of the current instance
     * @param p_configs
     *         Map with configurations to use for creating instances of modules
     */
    void init(final NodeRole p_currentInstanceNodeRole, final Map<String, ModuleConfig> p_configs, final ComponentProvider p_componentProvider) {
        for (ModuleConfig config : p_configs.values()) {
            ModuleContainer module = m_modules.get(config.getModuleClassName());

            if (module == null) {
                throw new RuntimeException("Cannot find module " + config.getModuleClassName() +
                        " for configuration instance " + config.getConfigClassName());
            }

            Annotation[] annotations = module.getModuleClass().getAnnotations();
            Module.Attributes attributes = null;

            for (Annotation annotation : annotations) {
                if (annotation instanceof Module.Attributes) {
                    attributes = (Module.Attributes) annotation;
                    break;
                }
            }

            if (attributes == null) {
                throw new IllegalStateException("Missing attributes for module " + module.getName());
            }

            if (p_currentInstanceNodeRole == NodeRole.SUPERPEER && attributes.supportsSuperpeer() ||
                    p_currentInstanceNodeRole == NodeRole.PEER && attributes.supportsPeer()) {
                LOGGER.debug("Creating instance of %s", module.getName());

                module.newModuleInstance(config, p_componentProvider);
            }
        }
    }

    /**
     * Get a module from the engine.
     *
     * @param <T>
     *         Type of the module class.
     * @param p_class
     *         Class of the module to get. If the module has different implementations, use the common
     *         interface or abstract class to get the registered instance.
     * @return Reference to the module if available, null otherwise
     */
    <T extends Module> T getModule(final Class<T> p_class) {
        T module = null;

        ModuleContainer moduleContainer = m_modules.get(p_class.getSimpleName());

        if (moduleContainer == null) {
            // check for any kind of instance of the specified class
            // we might have another interface/abstract class between the
            // class we request and an instance we could serve
            for (Map.Entry<String, ModuleContainer> entry : m_modules.entrySet()) {
                ModuleContainer mod = entry.getValue();

                if (p_class.isInstance(mod.getInstance())) {
                    module = p_class.cast(mod.getInstance());
                    break;
                }
            }
        } else {
            module = p_class.cast(moduleContainer.getInstance());
        }

        return module;
    }

    /**
     * Get all modules of a specific sub-type
     *
     * @param p_type
     *         Class of the sub-type, e.g. component or service
     * @param <T>
     *         Type of the sub-type
     * @return List of modules that match the specified sub-type
     */
    <T extends Module> List<T> getModules(final Class<T> p_type) {
        List<T> list = new ArrayList<>();

        for (ModuleContainer module : m_modules.values()) {
            Module mod = module.getInstance();

            // don't return non instanciated modules (non supported modules on current node type)
            if (mod != null) {
                list.add(p_type.cast(mod));
            }
        }

        return list;
    }

    /**
     * Internal data structure to keep track of a single module and it's metadata resources
     */
    @Data
    @Accessors(prefix = "m_")
    private static class ModuleContainer {
        private final String m_name;
        private final Class<? extends Module> m_moduleClass;
        private final Class<? extends ModuleConfig> m_configClass;

        private Module m_instance;

        /**
         * Constructor
         *
         * @param p_name
         *         Name of the module
         * @param p_moduleClass
         *         Class of the module
         * @param p_configClass
         *         Configuration class of the module
         */
        ModuleContainer(final String p_name, final Class<? extends Module> p_moduleClass,
                final Class<? extends ModuleConfig> p_configClass) {
            m_name = p_name;
            m_moduleClass = p_moduleClass;
            m_configClass = p_configClass;
        }

        /**
         * Create a new default configuration instance (can be called multiple times)
         *
         * @param p_moduleClassName
         *         Class name of the module
         * @return New default configuration instance
         */
        ModuleConfig newDefaultConfigInstance(final String p_moduleClassName) {
            // create new default configurations on each call
            try {
                if (!m_configClass.equals(ModuleConfig.class)) {
                    return m_configClass.getConstructor().newInstance();
                } else {
                    return m_configClass.getConstructor(String.class).newInstance(p_moduleClassName);
                }
            } catch (final Exception e) {
                throw new RuntimeException("Cannot create module config instance of " +
                        m_moduleClass.getSimpleName(), e);
            }
        }

        /**
         * Create a new module instance (can be called once, errors otherwise)
         *
         * @param p_config
         *         Configuration to use for module to instantiate
         * @return New module instance (also tracked internally)
         */
        Module newModuleInstance(final ModuleConfig p_config, final ComponentProvider p_componentProvider) {
            // allow single module instance, only
            if (m_instance != null) {
                throw new IllegalStateException("An instance of the module was already created: " +
                        m_instance.getName());
            }

            try {
                m_instance = m_moduleClass.getConstructor().newInstance();
            } catch (final Exception inner) {
                throw new RuntimeException("Cannot create module instance of " + m_moduleClass.getSimpleName(), inner);
            }

            m_instance.setConfig(p_config);

            return m_instance;
        }

        /**
         * Get the instance of the module (newModuleInstnace must have been called previously)
         *
         * @return The module instance
         */
        Module getModuleInstance() {
            if (m_instance == null) {
                throw new IllegalStateException("No instance of module created: " + m_moduleClass.getSimpleName());
            }

            return m_instance;
        }
    }
}
