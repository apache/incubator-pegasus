package com.xiaomi.infra.pegasus.tools;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.xiaomi.infra.pegasus.client.PException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** ZstdWrapper wraps the compress/decompress APIs of ZStd algorithm. */
public class ZstdWrapper {

  private ZstdWrapper() {}

  /**
   * Compresses the `src` and returns the compressed.
   *
   * @throws RuntimeException if compression failed.
   */
  public static byte[] compress(byte[] src) {
    return Zstd.compress(src);
  }

  /**
   * Decompresses the `src` and returns the original.
   *
   * @param src not null nor empty, or IllegalArgumentException will be thrown.
   * @throws PException if decompression failed, maybe your `src` is corrupted.
   */
  public static byte[] decompress(byte[] src) throws PException {
    if (src == null || src.length == 0) {
      throw new IllegalArgumentException("src is empty");
    }

    byte[] ret;
    long originalSize = Zstd.decompressedSize(src);
    if (originalSize > 0) {
      ret = new byte[(int) originalSize];
      long code = Zstd.decompress(ret, src);
      if (Zstd.isError(code)) {
        throw new PException("decompression failed: " + Zstd.getErrorName(code));
      }
      if (code != originalSize) {
        throw new PException("decompression failed");
      }
      return ret;
    }

    // fallback to decompress in streaming mode
    byte[] inBuf = new byte[1024];
    ByteArrayOutputStream decompressOutBuf = new ByteArrayOutputStream();
    try {
      ZstdInputStream decompress = new ZstdInputStream(new ByteArrayInputStream(src));
      while (true) {
        int n = decompress.read(inBuf);
        if (n <= 0) {
          break;
        }
        decompressOutBuf.write(inBuf, 0, n);
      }
      ret = decompressOutBuf.toByteArray();
    } catch (IOException e) {
      throw new PException("decompression failed: " + e.getMessage());
    }
    return ret;
  }
}
