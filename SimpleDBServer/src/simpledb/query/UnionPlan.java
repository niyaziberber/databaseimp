package simpledb.query;

import simpledb.record.Schema;

/**
 * Created by brylee on 11/8/16.
 */
public class UnionPlan implements Plan {
    Plan p1, p2;
    Schema schema = new Schema();

    public UnionPlan(Plan p1, Plan p2) {
        this.p1 = p1;
        this.p2 = p2;
        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
    }
    @Override
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        return new UnionScan(s1, s2);
    }

    @Override
    public int blocksAccessed() {
        return p1.blocksAccessed() + p2.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return p1.recordsOutput() + p2.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return p1.distinctValues(fldname) + p2.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
