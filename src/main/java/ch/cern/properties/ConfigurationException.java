package ch.cern.properties;

public class ConfigurationException extends Exception {

	private static final long serialVersionUID = -2597184362736139519L;

	public ConfigurationException(Exception e) {
		super(e);
	}

    public ConfigurationException(String msg) {
        super(msg);
    }
	
}
