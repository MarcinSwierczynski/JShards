package utils;

public class ShardsIterables {

	public static Integer sum(Iterable<Integer> iterable) {
		int sum = 0;
		for (Integer number : iterable) {
			sum += number;
		}
		return sum;
	}
	
}
