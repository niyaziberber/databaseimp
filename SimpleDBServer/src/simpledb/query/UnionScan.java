package simpledb.query;

/**
 * Created by brylee on 11/8/16.
 */
public class UnionScan implements Scan {
    private Scan s1, s2;
    private boolean pointerAtS1 = true;

    public UnionScan(Scan s1, Scan s2) {
        this.s1 = s1;
        this.s2 = s2;
    }

    @Override
    public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
    }

    @Override
    public boolean next() {
        if (s1.next()) {
            return true;
        }
        pointerAtS1 = false;
        return s2.next();
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }

    @Override
    public Constant getVal(String fldname) {
        return pointerAtS1 ? s1.getVal(fldname) : s2.getVal(fldname);
    }

    @Override
    public int getInt(String fldname) {
        return pointerAtS1 ? s1.getInt(fldname) : s2.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        return pointerAtS1 ? s1.getString(fldname) : s2.getString(fldname);

    }

    @Override
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) && s2.hasField(fldname);
    }
}
