package ch.cern;

public abstract class PropertiesSource extends Component {

	private static final long serialVersionUID = 4436444683021922084L;

	public PropertiesSource() {
		super(Component.Type.PROPERTIES_SOURCE);
	}
	
	public PropertiesSource(Class<? extends Component> subClass, String name) {
		super(Component.Type.PROPERTIES_SOURCE, subClass, name);
	}
	
	public abstract Properties load();

}
