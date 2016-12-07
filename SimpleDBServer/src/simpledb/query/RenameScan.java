package simpledb.query;

public class RenameScan implements Scan {
    private Scan s;
    private String oldfldname, newfldname;

    public RenameScan(Scan s, String oldfldname, String newfldname) {
        this.s = s;
        this.oldfldname = oldfldname;
        this.newfldname = newfldname;
    }

    public void beforeFirst() {
        s.beforeFirst();
    }

    public boolean next() {
        return s.next();
    }

    public void close() {
        s.close();
    }

    public Constant getVal(String fldname) {
        if (fldname.equals(newfldname))
            return s.getVal(oldfldname);
        else
            return s.getVal(fldname);
    }

    public int getInt(String fldname) {
        if (fldname.equals(newfldname))
            return s.getInt(oldfldname);
        else
            return s.getInt(fldname);
    }

    public String getString(String fldname) {
        if (fldname.equals(newfldname))
            return s.getString(oldfldname);
        else
            return s.getString(fldname);
    }

    public boolean hasField(String fldname) {
        if (fldname.equals(oldfldname))
            return false;
        else if (fldname.equals(newfldname))
            return true;
        else
            return s.hasField(fldname);
    }
}

