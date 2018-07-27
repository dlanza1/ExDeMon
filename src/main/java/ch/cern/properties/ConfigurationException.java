package ch.cern.properties;

import lombok.Getter;
import lombok.Setter;

public class ConfigurationException extends Exception {

    private static final long serialVersionUID = -2597184362736139519L;

    @Getter
    @Setter
    private String parameter;

    public ConfigurationException(Exception e) {
        super(e);
    }
    
    public ConfigurationException(String parameter, Exception e) {
        super(e);

        this.parameter = parameter;
    }

    public ConfigurationException(String parameter, String msg) {
        super(msg);

        this.parameter = parameter;
    }
    
    @Override
    public String toString() {
        if(parameter != null)
            return parameter + ": " + getMessage();
        else
            return getMessage();
    }

}
