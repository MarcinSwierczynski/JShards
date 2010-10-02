package shards;

public class ConfigurationProblemException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ConfigurationProblemException() {
		super();
	}

	public ConfigurationProblemException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConfigurationProblemException(String message) {
		super(message);
	}

	public ConfigurationProblemException(Throwable cause) {
		super(cause);
	}

}
