package shards;

public class AvgIndex {
	private int avgIndex;
	private int sumIndex;

	public AvgIndex(int avgIndex, int sumIndex) {
		this.avgIndex = avgIndex;
		this.sumIndex = sumIndex;
	}

	public int getAvgIndex() {
		return avgIndex;
	}

	public void setAvgIndex(int avgIndex) {
		this.avgIndex = avgIndex;
	}

	public int getSumIndex() {
		return sumIndex;
	}

	public void setSumIndex(int sumIndex) {
		this.sumIndex = sumIndex;
	}

}
