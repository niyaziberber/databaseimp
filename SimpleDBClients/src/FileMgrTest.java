import simpledb.server.SimpleDB;
import simpledb.file.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class FileMgrTest {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString("zdef12345".getBytes(StandardCharsets.US_ASCII)));
      SimpleDB.initFileMgr("testdb");
      Block blk = new Block("testfile", 0);
      Page pg = new Page();
      int[] offsets = new int[6];
      offsets[0] = 0;
      pg.setInt(offsets[0], 345);
      offsets[1] = offsets[0] + Page.INT_SIZE;
      pg.setString(offsets[1], "abc");
      System.out.println("size of abc = " + Page.STR_SIZE(3));
      offsets[2] = offsets[1] + Page.STR_SIZE(3);
      pg.setInt(offsets[2], 678);
      offsets[3] = offsets[2] + Page.INT_SIZE;
      pg.setString(offsets[3], "defghijklm");
      System.out.println("size of defghijklm = " + Page.STR_SIZE(10));
      offsets[4] = offsets[3] + Page.STR_SIZE(10);
      pg.setString(offsets[4], "nopqrstuvwxyz");
      System.out.println("size of mnopqrstuvwxyz = " + Page.STR_SIZE(13));
      
      pg.write(blk);
      
      Page pg2 = new Page();
      pg2.read(blk);
      System.out.println("value at offset " + offsets[0] + " = "
                            + pg2.getInt(offsets[0]));
      System.out.println("value at offset" + offsets[1] + " = "
            + pg2.getString(offsets[1]));
      System.out.println("value at offset" + offsets[2] + " = "
            + pg2.getInt(offsets[2]));
      System.out.println("value at offset" + offsets[3] + " = "
            + pg2.getString(offsets[3]));
      System.out.println("value at offset" + offsets[4] + " = "
            + pg2.getString(offsets[4]));
   }
}