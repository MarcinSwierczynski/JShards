package utils;

public class StringUtils {
	public static String stripLeadingAndTrailingQuotes(String str) {
		if (str.startsWith("\"")) {
			str = str.substring(1, str.length());
		}
		if (str.endsWith("\"")) {
			str = str.substring(0, str.length() - 1);
		}
		return str;
	}
	
	public static String getRegexFromSqlLike(String sqlLike) {
		return sqlLike.replaceAll("%", "\\.\\*").replaceAll("\\?", "\\.");
	}
}
