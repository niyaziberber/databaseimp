package simpledb.buffer;

import simpledb.file.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Queue<Buffer> unpinnedBuffers = new LinkedList<>();
   private Map<Block, Buffer> bufferMap = new HashMap<>();
   private Buffer[] bufferpool;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      for (int i=0; i<numbuffs; i++) {
         bufferpool[i] = new Buffer(i);
         unpinnedBuffers.add(bufferpool[i]);
      }
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool) {
         if (buff.isModifiedBy(txnum))
            buff.flush();
      }
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         bufferMap.put(blk, buff);
         buff.assignToBlock(blk);
      }
      buff.pin();
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      bufferMap.remove(buff.block());
      bufferMap.put(buff.block(), buff);
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) {
         unpinnedBuffers.add(buff);
      }
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return unpinnedBuffers.size();
   }
   
   private Buffer findExistingBuffer(Block blk) {
      Buffer b;
      if ((b = bufferMap.get(blk)) != null) {
         return b;
      }
      return null;
   }

   /**
    * chooses unpinned buffer from the queue.
    * Checks for isPinned because there can be a case
    * where the buffer in the queue becomes pinned
    * Case:: If there is already a buffer assigned
    *        to a block then that buffer is used. This
    *        buffer is allocated but unpinned, meaning
    *        it is in the unpinned queue. Since elements
    *        in the queue cannot be manipulated, this
    *        check is necessary.
    * @return unpinned buffer
    */
   private Buffer chooseUnpinnedBuffer() {
      Buffer b;
      while ((b = unpinnedBuffers.poll()) != null) {
         if (!b.isPinned())
            return b;
      }
      return null;
   }

   public String toString() {
      String s = "";
      for (int i = 0; i < bufferpool.length-1; i++) {
         s += bufferpool[i].toString() + "\n";
      }
      s += bufferpool[bufferpool.length-1].toString();
      return s;
   }
}
