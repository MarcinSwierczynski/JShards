package shards;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import utils.Calculation;

public class CalculationTest {

	private Calculation calculation;

	@Before
	public void setup() {
		calculation = new Calculation();
	}

	@Test
	public void add() {
		assertEquals(3d, calculation.add(1d, 2d), 0d);
		assertEquals(3d, calculation.add(1d, 2l), 0d);
		assertEquals(3d, calculation.add(1d, "2.0"), 0d);
		assertEquals(3d, calculation.add(1d, "2"), 0d);
		assertEquals(3d, calculation.add(1l, 2d), 0d);
		assertEquals(3l, calculation.add(1l, 2l));
		assertEquals(3l, calculation.add(1l, "2"));
	}

	@Test(expected = RuntimeException.class)
	public void addWrong1() {
		calculation.add(1l, "2.0");
	}

	@Test
	public void subtract() {
		assertEquals(3d, calculation.subtract(5d, 2d), 0d);
		assertEquals(3d, calculation.subtract(5d, 2l), 0d);
		assertEquals(3d, calculation.subtract(5d, "2.0"), 0d);
		assertEquals(3d, calculation.subtract(5d, "2"), 0d);
		assertEquals(3d, calculation.subtract(5l, 2d), 0d);
		assertEquals(3l, calculation.subtract(5l, 2l));
		assertEquals(3l, calculation.subtract(5l, "2"));
		assertEquals(3l, calculation.subtract("5.0", 2.0d), 0d);
	}

	@Test(expected = RuntimeException.class)
	public void subtractWrong1() {
		calculation.subtract(5l, "2.0");
	}

	@Test(expected = RuntimeException.class)
	public void subtractWrong2() {
		calculation.subtract("5.0", 2l);
	}

	@Test(expected = RuntimeException.class)
	public void subtractWrong3() {
		calculation.subtract("5.0", "2.0");
	}

	@Test
	public void multiply() {
	}

	@Test
	public void divide() {
	}

	@Test
	public void concat() {
	}
}
