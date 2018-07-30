package simpledb;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File f;

    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
           RandomAccessFile raf = new RandomAccessFile(f, "r");
           try {
               int pageSize = BufferPool.getPageSize();
               raf.seek(pid.getPageNumber() * pageSize);
               byte[] data = new byte[pageSize];
               raf.readFully(data);
               return new HeapPage((HeapPageId) pid, data);
           } finally {
               raf.close();
           }
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "w");
            try {
                int pageSize = BufferPool.getPageSize();
                raf.seek(page.getId().getPageNumber() * pageSize);
                raf.write(page.getPageData());
            } finally {
                raf.close();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)((f.length() + BufferPool.getPageSize() - 1) / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {

            private int pgNo = 0;

            private Iterator<Tuple> pageIter = null;

            private Iterator<Tuple> getPageIter() throws DbException, TransactionAbortedException {
                if (pgNo < 0 || pgNo >= numPages()) {
                    return null;
                }
                HeapPageId heapPageId = new HeapPageId(getId(), pgNo);
                HeapPage heapPage =
                        (HeapPage)(Database
                                .getBufferPool()
                                .getPage(tid, heapPageId, Permissions.READ_ONLY));
                return heapPage.iterator();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                rewind();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                while (pageIter != null) {
                    if (pageIter.hasNext()) {
                        return true;
                    }
                    pgNo++;
                    pageIter = getPageIter();
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return pageIter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pgNo = 0;
                pageIter = getPageIter();
            }

            @Override
            public void close() {
                pageIter = null;
            }
        };
    }

}

