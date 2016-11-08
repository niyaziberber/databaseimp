package simpledb.query;

import simpledb.record.Schema;

/**
 * Created by brylee on 11/6/16.
 */
public class RenamePlan implements Plan {
    private Plan p;
    private String oldField;
    private String newField;

    public RenamePlan(Plan p, String oldField, String newField) {
        this.p = p;
        this.oldField = oldField;
        this.newField = newField;
    }

    @Override
    public Scan open() {
        Scan s = p.open();
        return new RenameScan(s, oldField, newField);
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return p.distinctValues(getField(fldname));
    }

    @Override
    public Schema schema() {
        return p.schema();
    }

    private String getField(String field) {
        return field.equals(newField) ? oldField : field;
    }
}
