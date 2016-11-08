package simpledb.query;

/**
 * Created by brylee on 11/6/16.
 */
public class RenameScan implements Scan {
    private Scan s;
    private String oldField;
    private String newField;

    public RenameScan(Scan s, String oldField, String newField) {
        this.s = s;
        this.oldField = oldField;
        this.newField = newField;
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        return s.next();
    }

    @Override
    public void close() {
        s.close();
    }

    private String getField(String field) {
        return field.equals(newField) ? oldField : field;
    }

    @Override
    public Constant getVal(String fldname) {
        return s.getVal(getField(fldname));
    }

    @Override
    public int getInt(String fldname) {
        return s.getInt(getField(fldname));
    }

    @Override
    public String getString(String fldname) {
        return s.getString(getField(fldname));
    }

    @Override
    public boolean hasField(String fldname) {
        return s.hasField(getField(fldname));
    }
}
