package simpledb.query;

import simpledb.record.Schema;

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
      this.op = "=";
   }

   /**
    * Creates a new term that either compares two expressions
    * for > or <; or checks if an expression is null.
    * @param lhs  the LHS expression
    * @param op   the operator (<,>,isnull)
    * @param rhs  the RHS expression
    */
   public Term(Expression lhs, String op, Expression rhs) {
         this.lhs = lhs;
         this.rhs = rhs;
         this.op = op;
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
      int value = Integer.MAX_VALUE;

      if (lhs.isFieldName() && rhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         rhsName = rhs.asFieldName();
         value = Math.max(p.distinctValues(lhsName),
                         p.distinctValues(rhsName));
      }
      else if (lhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         value = p.distinctValues(lhsName);
      }
      else if (rhs.isFieldName()) {
         rhsName = rhs.asFieldName();
         value = p.distinctValues(rhsName);
      }
      // otherwise, the term equates constants
      else if (lhs.asConstant().equals(rhs.asConstant()))
         value = 1;
      return op.equals(">") || op.equals("<") ? value * 2 : op.equals("=") ? (int) (value * 1.11) : Integer.MAX_VALUE;
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
      if (op.equals("=")) {
         if (lhs.isFieldName() &&
                 lhs.asFieldName().equals(fldname) &&
                 rhs.isConstant())
            return rhs.asConstant();
         else if (rhs.isFieldName() &&
                 rhs.asFieldName().equals(fldname) &&
                 lhs.isConstant())
            return lhs.asConstant();
      }
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
      if (op.equals("=")) {
         if (lhs.isFieldName() &&
                 lhs.asFieldName().equals(fldname) &&
                 rhs.isFieldName())
            return rhs.asFieldName();
         else if (rhs.isFieldName() &&
                 rhs.asFieldName().equals(fldname) &&
                 lhs.isFieldName())
            return lhs.asFieldName();
      }
      return null;
   }

   /**
    * Returns true if both of the term's expressions
    * apply to the specified schema.
    * @param sch the schema
    * @return true if both expressions apply to the schema
    */
   public boolean appliesTo(Schema sch) {
      if (op.equals("isnull")) return lhs.appliesTo(sch);
      return lhs.appliesTo(sch) && rhs.appliesTo(sch);
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
      if (op.equals("isnull"))
         return lhsval instanceof NullConstant;
      Constant rhsval = rhs.evaluate(s);

      if (lhsval instanceof NullConstant || rhsval instanceof NullConstant)
         return false;


      int compared = lhsval.compareTo(rhsval);

      return op.equals(">") ? (compared > 0) : (op.equals("<") ? (compared < 0) : (compared == 0));
   }

   public String toString() {
      if (op.equals("isnull"))
         return lhs.toString() + " is null";
      return lhs.toString() + op + rhs.toString();
   }
}
