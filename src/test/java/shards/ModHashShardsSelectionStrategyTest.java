package shards;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import shards.QueryVisitor.ParameterInfoBean;

public class ModHashShardsSelectionStrategyTest {

	private ModHashShardsSelectionStrategy strategy;

	@Before
	public void init() {
		strategy = new ModHashShardsSelectionStrategy();
		strategy.setColumn("id");
		strategy.setConfiguration(JUnitHelper.mockConfiguration());
	}

	@Test
	public void selectShards() {
		ParameterInfoBean info = createParameterInfoBean(1l);
		Set<String> selectedFor1 = strategy.selectShards(info);
		info = createParameterInfoBean(3l);
		Set<String> selectedFor3 = strategy.selectShards(info);
		assertEquals(selectedFor1, selectedFor3);
		
		info = createParameterInfoBean(2l);
		Set<String> selectedFor2 = strategy.selectShards(info);
		assertFalse(selectedFor1.equals(selectedFor2));
	}

	private ParameterInfoBean createParameterInfoBean(Long value) {
		ParameterInfoBean info = new ParameterInfoBean("id", "test", Operation.SELECT);
		info.setValue(value);
		return info;
	}

}
