package utils;

public class Types {

	/**
	 * If string is not a long then null is returned
	 */
	public static Long castToLong(String string) {
		try {
			return Long.parseLong(string);
		} catch (NumberFormatException e) {
			return null;
		}
	}

	/**
	 * If string is not a number then null is returned
	 */
	public static Double castToDouble(String string) {
		try {
			return Double.parseDouble(string);
		} catch (NumberFormatException e) {
			return null;
		}
	}
	
}
