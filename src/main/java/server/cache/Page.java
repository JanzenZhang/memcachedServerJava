/**
 * @author Dilip Simha
 */
package server.cache;

public class Page {
    private final int pageSize;

    private byte[] data;

    public Page(int pageSize) {
        this.pageSize = pageSize;
        data = new byte[pageSize];
    }

    public int getPageSize() {
        return pageSize;
    }

    public byte[] getData() {
        return data;
    }
}
