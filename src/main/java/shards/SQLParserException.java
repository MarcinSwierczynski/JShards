package shards;

import java.sql.SQLException;

public class SQLParserException extends SQLException {

	private static final long serialVersionUID = 1L;

	public SQLParserException() {
		super();
	}

	public SQLParserException(String message, Throwable cause) {
		super(message, cause);
	}

	public SQLParserException(String message) {
		super(message);
	}

	public SQLParserException(Throwable cause) {
		super(cause);
	}

}
