package Main;

public class PageId {
  private int fileIdx;
  private int pageIdx;

  public PageId(int fileIdx, int pageIdx) {
    this.fileIdx = fileIdx;
    this.pageIdx = pageIdx;
  }

  public int getFileIdx() {
    return fileIdx;
  }

  public int getPageIdx() {
    return pageIdx;
  }

  public void setFileIdx(int fileIdx) {
    this.fileIdx = fileIdx;
  }

  public void setPageIdx(int pageIdx) {
    this.pageIdx = pageIdx;
  }

  @Override
  public String toString() {
    return "PageId(FileIdx=" + fileIdx + ", PageIdx=" + pageIdx + ")";
  }

  @Override
  public boolean equals(Object obj) {

    if (this == obj)
      return true;
    if (obj == null || getClass() != obj.getClass())
      return false;

    PageId pageId = (PageId) obj;
    return fileIdx == pageId.fileIdx && pageIdx == pageId.pageIdx;
  }

  @Override
  public int hashCode() {
    return 31 * fileIdx + pageIdx;
  }
}