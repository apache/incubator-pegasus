/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pegasus.tools;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.pegasus.client.PException;

/** ZstdWrapper wraps the compress/decompress APIs of ZStd algorithm. */
public class ZstdWrapper {

  private ZstdWrapper() {}

  /**
   * try decompress the `src`, return `src` directly if failed
   *
   * @param src the origin sending value
   * @return the decompressed value.
   */
  public static byte[] tryDecompress(byte[] src) {
    byte[] decompressedValue;
    try {
      decompressedValue = decompress(src);
    } catch (Exception e) {
      // decompress fail
      decompressedValue = src;
    }
    return decompressedValue;
  }

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
    try (ByteArrayOutputStream decompressOutBuf = new ByteArrayOutputStream();
        ZstdInputStream decompress = new ZstdInputStream(new ByteArrayInputStream(src))) {
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
