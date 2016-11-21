package simpledb.query;

import java.io.*;
import java.util.*;
import java.text.*;
import java.math.*;
import java.util.regex.*;

/**
 * Created by brylee on 11/21/16.
 */
public class NullConstant implements Constant {
    public String toString() {
        return "null";
    }

    public Object asJavaVal() {
        return null;
    }

    public int compareTo(Constant o) {
        return 0;
    }
}
