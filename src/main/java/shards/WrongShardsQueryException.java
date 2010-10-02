package shards;

/**
 * Thrown when the passed query cannot be run in shards architecture
 */
public class WrongShardsQueryException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public WrongShardsQueryException() {
		super();
	}

	public WrongShardsQueryException(String message, Throwable cause) {
		super(message, cause);
	}

	public WrongShardsQueryException(String message) {
		super(message);
	}

	public WrongShardsQueryException(Throwable cause) {
		super(cause);
	}

}
