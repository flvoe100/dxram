package de.hhu.bsinfo.dxram.nameservice;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;

/**
 * Config for the NameserviceComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class NameserviceComponentConfig extends DXRAMComponentConfig {
    @Expose
    private String m_type = "NAME";

    /**
     * Constructor
     */
    public NameserviceComponentConfig() {
        super(NameserviceComponent.class, false, true);
    }

    /**
     * Type of name service string converter to use to convert name service keys (available: NAME and INT)
     */
    public String getType() {
        return m_type;
    }
}
