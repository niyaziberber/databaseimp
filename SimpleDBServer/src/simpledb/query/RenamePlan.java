package simpledb.query;

import simpledb.record.Schema;

public class RenamePlan implements Plan {
    private Plan p;
    private String oldfldname, newfldname;
    private Schema schema = new Schema();

    public RenamePlan(Plan p, String oldfldname, String newfldname) {
        this.p = p;
        this.oldfldname = oldfldname;
        this.newfldname = newfldname;
        Schema oldschema = p.schema();
        for (String fldname : oldschema.fields())
            if (!fldname.equals(oldfldname))
                schema.add(fldname, oldschema);
        int fldtype = oldschema.type(oldfldname);
        int fldlen  = oldschema.length(oldfldname);
        schema.addField(newfldname, fldtype, fldlen);
    }

    public Scan open() {
        Scan s = p.open();
        return new RenameScan(s, oldfldname, newfldname);
    }

    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    public int recordsOutput() {
        return p.recordsOutput();
    }

    public int distinctValues(String fldname) {
        if (fldname.equals(newfldname))
            return p.distinctValues(oldfldname);
        else
            return p.distinctValues(fldname);
    }

    public Schema schema() {
        return schema;
    }
}

