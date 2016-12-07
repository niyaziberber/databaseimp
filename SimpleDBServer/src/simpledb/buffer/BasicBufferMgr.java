package simpledb.buffer;

import simpledb.file.*;
import java.util.*;

// Most everything is modified for HW3
// Note that the unpinned queue will contain a pinned
// buffer when an existing unpinned block is pinned.

class BasicBufferMgr {
   private Map<Block,Buffer>allocated = new HashMap<Block,Buffer>();
   private Queue<Buffer>     unpinned = new LinkedList<Buffer>();
   private int numAvailable;
   
   BasicBufferMgr(int numbuffs) {
   		numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++)
         unpinned.add(new Buffer(i));
   }
   
   synchronized void flushAll(int txnum) {
      for (Buffer buff : allocated.values())
         if (buff.isModifiedBy(txnum))
             buff.flush();
   }

   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         allocated.put(blk, buff);  //add to the allocated map
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }
   
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      allocated.put(buff.block(), buff);  // add to the allocated map
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }
 
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) {
         numAvailable ++;
         unpinned.add(buff);  // add to the end of the queue
      }
   }
   
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
      return allocated.get(blk);
   }
   
   private Buffer chooseUnpinnedBuffer() {
   		while (!unpinned.isEmpty()) {
   			Buffer buff = unpinned.remove();
   			if (!buff.isPinned()) {
   				allocated.remove(buff.block());	
   				return buff;
   			}
   		}
   		return null;
   }
   
   // print buffers in ascending order
   public String toString() {
      String result = "";
      List<Buffer> buffs = new ArrayList<Buffer>(allocated.values());
      Collections.sort(buffs, new Comparator<Buffer>() {
      				public int compare(Buffer b1, Buffer b2) {
      					return b1.toString().compareTo(b2.toString());
      				}
      			} );
      for (Buffer buff : buffs)
         result += buff.toString() + "\n";
      return result + "============";
   }
}
