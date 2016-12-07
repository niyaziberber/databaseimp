package simpledb.query;

public class NullConstant implements Constant {

	public Object asJavaVal() {
		return null;
	}

	public boolean equals(Object obj) {
		return false;
	}

	// This method should never be cqlled.
	public int compareTo(Constant c) {
		return -1;
	}

	public int hashCode() {
		return 0;
	}

	public String toString() {
		return "null";
	}
}
