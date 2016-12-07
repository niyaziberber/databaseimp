package simpledb.query;

import simpledb.record.Schema;

// Most methods modified for hw8 

/**
 * A term is a comparison between two expressions.
 * @author Edward Sciore
 *
 */
public class Term {
	private Expression lhs, rhs;
	private String op;

	/**
	 * Creates a new term that compares two expressions
	 * for equality.
	 * @param lhs  the LHS expression
	 * @param rhs  the RHS expression
	 */
	public Term(Expression lhs, Expression rhs) {
		this.lhs = lhs;
		this.rhs = rhs;

		// added for hw8
		this.op = "=";
	}

	// Added for hw8
	public Term(Expression lhs, String op, Expression rhs) {
		this.lhs = lhs;
		this.op = op;
		this.rhs = rhs;
	}

	/**
	 * Calculates the extent to which selecting on the term reduces 
	 * the number of records output by a query.
	 * For example if the reduction factor is 2, then the
	 * term cuts the size of the output in half.
	 * @param p the query's plan
	 * @return the integer reduction factor.
	 */
	public int reductionFactor(Plan p) {
		String lhsName, rhsName;

		// added for hw 8
		if (op.equals("<") || op.equals(">"))
			return 2;
		
		// added for hw 8
		if (op.equals("isnull"))
			return 10;

		if (lhs.isFieldName() && rhs.isFieldName()) {
			lhsName = lhs.asFieldName();
			rhsName = rhs.asFieldName();
			return Math.max(p.distinctValues(lhsName),
					p.distinctValues(rhsName));
		}
		if (lhs.isFieldName()) {
			lhsName = lhs.asFieldName();
			return p.distinctValues(lhsName);
		}
		if (rhs.isFieldName()) {
			rhsName = rhs.asFieldName();
			return p.distinctValues(rhsName);
		}
		// otherwise, the term equates constants
		if (lhs.asConstant().equals(rhs.asConstant()))
			return 1;
		else
			return Integer.MAX_VALUE;
	}

	/**
	 * Determines if this term is of the form "F=c"
	 * where F is the specified field and c is some constant.
	 * If so, the method returns that constant.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return either the constant or null
	 */
	public Constant equatesWithConstant(String fldname) {

		// added for hw 8
		if (op.equals("<") || op.equals(">") || op.equals("isnull"))
			return null;

		if (lhs.isFieldName() &&
				lhs.asFieldName().equals(fldname) &&
				rhs.isConstant())
			return rhs.asConstant();
		else if (rhs.isFieldName() &&
				rhs.asFieldName().equals(fldname) &&
				lhs.isConstant())
			return lhs.asConstant();
		else
			return null;
	}

	/**
	 * Determines if this term is of the form "F1=F2"
	 * where F1 is the specified field and F2 is another field.
	 * If so, the method returns the name of that field.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return either the name of the other field, or null
	 */
	public String equatesWithField(String fldname) {

		// added for hw 8
		if (op.equals("<") || op.equals(">") || op.equals("isnull"))
			return null;

		if (lhs.isFieldName() &&
				lhs.asFieldName().equals(fldname) &&
				rhs.isFieldName())
			return rhs.asFieldName();
		else if (rhs.isFieldName() &&
				rhs.asFieldName().equals(fldname) &&
				lhs.isFieldName())
			return lhs.asFieldName();
		else
			return null;
	}

	/**
	 * Returns true if both of the term's expressions
	 * apply to the specified schema.
	 * @param sch the schema
	 * @return true if both expressions apply to the schema
	 */
	public boolean appliesTo(Schema sch) {
		// modified for hw8
		boolean ok = lhs.appliesTo(sch);
		if (!op.equals("isnull"))
			ok = ok && rhs.appliesTo(sch);
		return ok;
	}

	/**
	 * Returns true if both of the term's expressions
	 * evaluate to the same constant,
	 * with respect to the specified scan.
	 * @param s the scan
	 * @return true if both expressions have the same value in the scan
	 */
	public boolean isSatisfied(Scan s) {
		Constant lhsval = lhs.evaluate(s);
		// added for hw 8
		Constant rhsval;
		if (op.equals("isnull"))
			rhsval = null;
		else
			rhsval = rhs.evaluate(s);

		// added for hw 8
		if (op.equals("isnull")) 
			return lhsval instanceof NullConstant;

		// added for hw 8
		if (lhsval instanceof NullConstant ||
			 rhsval instanceof NullConstant)
			return false;
		else if (op.equals("="))
			return lhsval.equals(rhsval);
		else if (op.equals("<"))
			return lhsval.compareTo(rhsval) < 0;
		else if (op.equals(">"))
			return lhsval.compareTo(rhsval) > 0;
		else return false;       
	}

	public String toString() {
		// modified for hw 8
		if (op.equals("isnull"))
			return lhs.toString() + " is null ";
		else
			return lhs.toString() + op + rhs.toString();
	}
}
