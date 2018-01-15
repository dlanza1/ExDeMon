package ch.cern.properties;

import java.io.IOException;

public class ConfigurationException extends Exception {

	private static final long serialVersionUID = -2597184362736139519L;

	public ConfigurationException(IOException e) {
		super(e);
	}

    public ConfigurationException(String msg) {
        super(msg);
    }
	
}
