package com.linkedin.gradle.azkaban;

import org.apache.http.HttpEntity;
import org.apache.http.entity.HttpEntityWrapper;


public class ProgressHttpEntityWrapper extends HttpEntityWrapper {

  private final ProgressCallback _progressCallback;

  public static interface ProgressCallback {
    public void progress(float progress);
  }

  public ProgressHttpEntityWrapper(final HttpEntity entity, final ProgressCallback progressCallback) {
    super(entity);
    this._progressCallback = progressCallback;
  }

  @Override
  public void writeTo(final OutputStream out) throws IOException {
    this.wrappedEntity.writeTo(out instanceof ProgressFilterOutputStream ? out : new ProgressFilterOutputStream(out, this._progressCallback, getContentLength()));
  }

  static class ProgressFilterOutputStream extends FilterOutputStream {

    private final ProgressCallback _progressCallback;
    private long _transferred;
    private long _totalBytes;

    ProgressFilterOutputStream(final OutputStream out, final ProgressCallback progressCallback, final long totalBytes) {
      super(out);
      this._progressCallback = progressCallback;
      this._transferred = 0;
      this._totalBytes = totalBytes;
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
      out.write(b, off, len);
      this._transferred += len;
      this._progressCallback.progress(getCurrentProgress());
    }

    @Override
    public void write(final int b) throws IOException {
      out.write(b);
      this._transferred++;
      this._progressCallback.progress(getCurrentProgress());
    }

    private float getCurrentProgress() {
      return ((float) this._transferred / this._totalBytes) * 100;
    }
  }

}
