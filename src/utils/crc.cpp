/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <cstdio>
#include "utils/crc.h"

namespace dsn {
namespace utils {

template <typename uintxx_t, uintxx_t uPoly>
struct crc_generator
{
    typedef uintxx_t uint;
    static const uintxx_t MSB = ((uintxx_t)1) << (8 * sizeof(uintxx_t) - 1);
    static const uintxx_t POLY = uPoly;
    static uintxx_t _crc_table[256];
    static uintxx_t _uX2N[64];

    //
    // compute CRC
    //
    static uintxx_t compute(const void *pSrc, size_t uSize, uintxx_t uCrc)
    {
        const uint8_t *pData = (const uint8_t *)pSrc;
        size_t uBytes;

        uCrc = ~uCrc;

        while (uSize > 15) {
            uBytes = 0x80000000u;
            if (uBytes > uSize)
                uBytes = uSize;
            uSize -= uBytes;

            for (; uBytes > 15; uBytes -= 16, pData += 16) {
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[0])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[1])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[2])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[3])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[4])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[5])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[6])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[7])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[8])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[9])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[10])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[11])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[12])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[13])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[14])] ^ (uCrc >> 8);
                uCrc = _crc_table[(uint8_t)(uCrc ^ pData[15])] ^ (uCrc >> 8);
            }

            uSize += uBytes;
        }

        for (uBytes = uSize; uBytes > 0; uBytes -= 1, pData += 1)
            uCrc = _crc_table[(uint8_t)(uCrc ^ pData[0])] ^ (uCrc >> 8);

        uCrc = ~uCrc;

        return (uCrc);
    };

    //
    // Returns (a * b) mod POLY.
    // "a" and "b" are represented in "reversed" order -- LSB is x**(XX-1) coefficient, MSB is x^0
    // coefficient.
    // "POLY" is represented in the same manner except for omitted x**XX coefficient
    //
    static uintxx_t MulPoly(uintxx_t a, uintxx_t b)
    {
        uintxx_t r;

        if (a == 0)
            return (0);

        r = 0;
        do {
            if (a & MSB)
                r ^= b;

            if (b & 1)
                b = (b >> 1) ^ POLY;
            else
                b >>= 1;

            a <<= 1;
        } while (a != 0);

        return (r);
    };

    //
    // Returns (x ** (8*uSize)) mod POLY
    //
    static uintxx_t ComputeX_N(uint64_t uSize)
    {
        size_t i;
        uintxx_t r;

        r = MSB; // r = 1
        for (i = 0; uSize != 0; uSize >>= 1, i += 1) {
            if (uSize & 1)
                r = MulPoly(r, _uX2N[i]);
        }

        return (r);
    };

    //
    // Allows to change initial CRC value
    //
    static uintxx_t ConvertInitialCrc(uintxx_t uNew, uintxx_t uOld, uintxx_t uCrc, size_t uSize)
    {
        //
        // CRC (A, uSize, uCrc) = (uCrc * x**uSize + A * x**XX) mod POLY (let's forget about double
        // NOTs of uCrc)
        //
        // we know uCrc(uOld) = (uOld * x**uSize + A * x**XX) mod POLY; we need to compute
        // uCrc(uNew) = (uNew * x**uSize + A * x**XX) mod POLY
        //
        // uCrc(uNew) = uCrc(Old) + (uNew - uOld) * x**uSize)
        //

        uNew ^= uOld;
        uOld = ComputeX_N(uSize);
        uOld = MulPoly(uOld, uNew);
        uCrc ^= uOld;

        return (uCrc);
    };

    //
    // Given
    //      uFinalCrcA = ComputeCrc (A, uSizeA, uInitialCrcA)
    // and
    //      uFinalCrcB = ComputeCrc (B, uSizeB, uInitialCrcB),
    // compute CRC of concatenation of A and B
    //      uFinalCrcAB = ComputeCrc (AB, uSizeA + uSizeB, uInitialCrcAB)
    // without touching A and B
    //
    // NB: uSizeA and/or uSizeB may be 0s (this trick may be used to "recompute" CRC for another
    // initial value)
    //

    static uintxx_t concatenate(uintxx_t uInitialCrcAB,
                                uintxx_t uInitialCrcA,
                                uintxx_t uFinalCrcA,
                                uint64_t uSizeA,
                                uintxx_t uInitialCrcB,
                                uintxx_t uFinalCrcB,
                                uint64_t uSizeB)
    {
        uintxx_t uX_nA, uX_nB, uFinalCrcAB;

        //
        // Crc (X, uSizeX, uInitialCrcX) = ~(((~uInitialCrcX) * x**uSizeX + X * x**XX) mod POLY)
        //

        //
        // first, convert CRC's to canonical values getting rid of double bitwise NOT around uCrc
        //
        uInitialCrcAB = ~uInitialCrcAB;
        uInitialCrcA = ~uInitialCrcA;
        uFinalCrcA = ~uFinalCrcA;
        uInitialCrcB = ~uInitialCrcB;
        uFinalCrcB = ~uFinalCrcB;

        //
        // convert uFinalCrcX into canonical form, so that
        //      uFinalCrcX = (X * x**XX) mod POLY
        //
        uX_nA = ComputeX_N(uSizeA);
        uFinalCrcA ^= MulPoly(uX_nA, uInitialCrcA);
        uX_nB = ComputeX_N(uSizeB);
        uFinalCrcB ^= MulPoly(uX_nB, uInitialCrcB);

        //
        // we know
        //      uFinalCrcA = (A * x**XX) mod POLY
        //      uFinalCrcB = (B * x**XX) mod POLY
        // and need to compute
        //      uFinalCrcAB = (AB * x**XX) mod POLY =
        //                  = ((A * x**uSizeB + B) * x**XX) mod POLY =
        //                  = (A * x**XX) * x**uSizeB + B * x**XX mod POLY =
        //                  = uFinalCrcB + (uFinalCrcA * x**uSizeB) mod POLY
        //

        uFinalCrcAB = uFinalCrcB ^ MulPoly(uFinalCrcA, uX_nB);

        //
        // Finally, adjust initial value; we have
        //      uFinalCrcAB = (AB * x**XX) mod POLY
        // but want to have
        //      uFinalCrcAB = (UInitialCrcAB * x**(uSizeA + uSizeB) + AB * x**XX) mod POLY
        //

        uFinalCrcAB ^= MulPoly(uInitialCrcAB, MulPoly(uX_nA, uX_nB));

        // convert back to double NOT
        uFinalCrcAB = ~uFinalCrcAB;

        return (uFinalCrcAB);
    };

    static void InitializeTables(void)
    {
        size_t i, j;
        uintxx_t k;

        _uX2N[0] = MSB >> 8;
        for (i = 1; i < sizeof(_uX2N) / sizeof(_uX2N[0]); ++i)
            _uX2N[i] = MulPoly(_uX2N[i - 1], _uX2N[i - 1]);

        for (i = 0; i < 256; ++i) {
            k = (uintxx_t)i;
            for (j = 0; j < 8; ++j) {
                if (k & 1)
                    k = (k >> 1) ^ POLY;
                else
                    k = (k >> 1);
            }
            _crc_table[i] = k;
        }
    }

    static void PrintTables(char *pTypeName, char *pClassName)
    {
        size_t i, w;

        InitializeTables();

        printf("%s %s::_uX2N[sizeof (%s::_uX2N) / sizeof (%s::_uX2N[0])] = {",
               pTypeName,
               pClassName,
               pClassName,
               pClassName);
        for (i = w = 0; i < sizeof(_uX2N) / sizeof(_uX2N[0]); ++i) {
            if (i != 0)
                printf(",");
            if (w == 0)
                printf("\n   ");
            printf(" 0x%0*llx", static_cast<int>(sizeof(uintxx_t) * 2), (uint64_t)_uX2N[i]);
            w = (w + sizeof(uintxx_t)) & 31;
        }
        printf("\n};\n\n");

        printf("%s %s::_crc_table[sizeof (%s::_crc_table) / sizeof (%s::_crc_table[0])] = {",
               pTypeName,
               pClassName,
               pClassName,
               pClassName);
        for (i = w = 0; i < sizeof(_crc_table) / sizeof(_crc_table[0]); ++i) {
            if (i != 0)
                printf(",");
            if (w == 0)
                printf("\n   ");
            printf(" 0x%0*llx", static_cast<int>(sizeof(uintxx_t) * 2), (uint64_t)_crc_table[i]);
            w = (w + sizeof(uintxx_t)) & 31;
        }
        printf("\n};\n\n");
    };
};

#define BIT64(n) (1ull << (63 - (n)))
#define crc64_POLY                                                                                 \
    (BIT64(63) + BIT64(61) + BIT64(59) + BIT64(58) + BIT64(56) + BIT64(55) + BIT64(52) +           \
     BIT64(49) + BIT64(48) + BIT64(47) + BIT64(46) + BIT64(44) + BIT64(41) + BIT64(37) +           \
     BIT64(36) + BIT64(34) + BIT64(32) + BIT64(31) + BIT64(28) + BIT64(26) + BIT64(23) +           \
     BIT64(22) + BIT64(19) + BIT64(16) + BIT64(13) + BIT64(12) + BIT64(10) + BIT64(9) + BIT64(6) + \
     BIT64(4) + BIT64(3) + BIT64(0))

#define BIT32(n) (1u << (31 - (n)))
#define crc32_POLY                                                                                 \
    (BIT32(28) + BIT32(27) + BIT32(26) + BIT32(25) + BIT32(23) + BIT32(22) + BIT32(20) +           \
     BIT32(19) + BIT32(18) + BIT32(14) + BIT32(13) + BIT32(11) + BIT32(10) + BIT32(9) + BIT32(8) + \
     BIT32(6) + BIT32(0))

typedef crc_generator<uint32_t, crc32_POLY> crc32;
typedef crc_generator<uint64_t, crc64_POLY> crc64;

template <>
uint32_t crc32::_uX2N[sizeof(crc32::_uX2N) / sizeof(crc32::_uX2N[0])] = {
    0x00800000, 0x00008000, 0x82f63b78, 0x6ea2d55c, 0x18b8ea18, 0x510ac59a, 0xb82be955, 0xb8fdb1e7,
    0x88e56f72, 0x74c360a4, 0xe4172b16, 0x0d65762a, 0x35d73a62, 0x28461564, 0xbf455269, 0xe2ea32dc,
    0xfe7740e6, 0xf946610b, 0x3c204f8f, 0x538586e3, 0x59726915, 0x734d5309, 0xbc1ac763, 0x7d0722cc,
    0xd289cabe, 0xe94ca9bc, 0x05b74f3f, 0xa51e1f42, 0x40000000, 0x20000000, 0x08000000, 0x00800000,
    0x00008000, 0x82f63b78, 0x6ea2d55c, 0x18b8ea18, 0x510ac59a, 0xb82be955, 0xb8fdb1e7, 0x88e56f72,
    0x74c360a4, 0xe4172b16, 0x0d65762a, 0x35d73a62, 0x28461564, 0xbf455269, 0xe2ea32dc, 0xfe7740e6,
    0xf946610b, 0x3c204f8f, 0x538586e3, 0x59726915, 0x734d5309, 0xbc1ac763, 0x7d0722cc, 0xd289cabe,
    0xe94ca9bc, 0x05b74f3f, 0xa51e1f42, 0x40000000, 0x20000000, 0x08000000, 0x00800000, 0x00008000};

template <>
uint32_t crc32::_crc_table[sizeof(crc32::_crc_table) / sizeof(crc32::_crc_table[0])] = {
    0x00000000, 0xf26b8303, 0xe13b70f7, 0x1350f3f4, 0xc79a971f, 0x35f1141c, 0x26a1e7e8, 0xd4ca64eb,
    0x8ad958cf, 0x78b2dbcc, 0x6be22838, 0x9989ab3b, 0x4d43cfd0, 0xbf284cd3, 0xac78bf27, 0x5e133c24,
    0x105ec76f, 0xe235446c, 0xf165b798, 0x030e349b, 0xd7c45070, 0x25afd373, 0x36ff2087, 0xc494a384,
    0x9a879fa0, 0x68ec1ca3, 0x7bbcef57, 0x89d76c54, 0x5d1d08bf, 0xaf768bbc, 0xbc267848, 0x4e4dfb4b,
    0x20bd8ede, 0xd2d60ddd, 0xc186fe29, 0x33ed7d2a, 0xe72719c1, 0x154c9ac2, 0x061c6936, 0xf477ea35,
    0xaa64d611, 0x580f5512, 0x4b5fa6e6, 0xb93425e5, 0x6dfe410e, 0x9f95c20d, 0x8cc531f9, 0x7eaeb2fa,
    0x30e349b1, 0xc288cab2, 0xd1d83946, 0x23b3ba45, 0xf779deae, 0x05125dad, 0x1642ae59, 0xe4292d5a,
    0xba3a117e, 0x4851927d, 0x5b016189, 0xa96ae28a, 0x7da08661, 0x8fcb0562, 0x9c9bf696, 0x6ef07595,
    0x417b1dbc, 0xb3109ebf, 0xa0406d4b, 0x522bee48, 0x86e18aa3, 0x748a09a0, 0x67dafa54, 0x95b17957,
    0xcba24573, 0x39c9c670, 0x2a993584, 0xd8f2b687, 0x0c38d26c, 0xfe53516f, 0xed03a29b, 0x1f682198,
    0x5125dad3, 0xa34e59d0, 0xb01eaa24, 0x42752927, 0x96bf4dcc, 0x64d4cecf, 0x77843d3b, 0x85efbe38,
    0xdbfc821c, 0x2997011f, 0x3ac7f2eb, 0xc8ac71e8, 0x1c661503, 0xee0d9600, 0xfd5d65f4, 0x0f36e6f7,
    0x61c69362, 0x93ad1061, 0x80fde395, 0x72966096, 0xa65c047d, 0x5437877e, 0x4767748a, 0xb50cf789,
    0xeb1fcbad, 0x197448ae, 0x0a24bb5a, 0xf84f3859, 0x2c855cb2, 0xdeeedfb1, 0xcdbe2c45, 0x3fd5af46,
    0x7198540d, 0x83f3d70e, 0x90a324fa, 0x62c8a7f9, 0xb602c312, 0x44694011, 0x5739b3e5, 0xa55230e6,
    0xfb410cc2, 0x092a8fc1, 0x1a7a7c35, 0xe811ff36, 0x3cdb9bdd, 0xceb018de, 0xdde0eb2a, 0x2f8b6829,
    0x82f63b78, 0x709db87b, 0x63cd4b8f, 0x91a6c88c, 0x456cac67, 0xb7072f64, 0xa457dc90, 0x563c5f93,
    0x082f63b7, 0xfa44e0b4, 0xe9141340, 0x1b7f9043, 0xcfb5f4a8, 0x3dde77ab, 0x2e8e845f, 0xdce5075c,
    0x92a8fc17, 0x60c37f14, 0x73938ce0, 0x81f80fe3, 0x55326b08, 0xa759e80b, 0xb4091bff, 0x466298fc,
    0x1871a4d8, 0xea1a27db, 0xf94ad42f, 0x0b21572c, 0xdfeb33c7, 0x2d80b0c4, 0x3ed04330, 0xccbbc033,
    0xa24bb5a6, 0x502036a5, 0x4370c551, 0xb11b4652, 0x65d122b9, 0x97baa1ba, 0x84ea524e, 0x7681d14d,
    0x2892ed69, 0xdaf96e6a, 0xc9a99d9e, 0x3bc21e9d, 0xef087a76, 0x1d63f975, 0x0e330a81, 0xfc588982,
    0xb21572c9, 0x407ef1ca, 0x532e023e, 0xa145813d, 0x758fe5d6, 0x87e466d5, 0x94b49521, 0x66df1622,
    0x38cc2a06, 0xcaa7a905, 0xd9f75af1, 0x2b9cd9f2, 0xff56bd19, 0x0d3d3e1a, 0x1e6dcdee, 0xec064eed,
    0xc38d26c4, 0x31e6a5c7, 0x22b65633, 0xd0ddd530, 0x0417b1db, 0xf67c32d8, 0xe52cc12c, 0x1747422f,
    0x49547e0b, 0xbb3ffd08, 0xa86f0efc, 0x5a048dff, 0x8ecee914, 0x7ca56a17, 0x6ff599e3, 0x9d9e1ae0,
    0xd3d3e1ab, 0x21b862a8, 0x32e8915c, 0xc083125f, 0x144976b4, 0xe622f5b7, 0xf5720643, 0x07198540,
    0x590ab964, 0xab613a67, 0xb831c993, 0x4a5a4a90, 0x9e902e7b, 0x6cfbad78, 0x7fab5e8c, 0x8dc0dd8f,
    0xe330a81a, 0x115b2b19, 0x020bd8ed, 0xf0605bee, 0x24aa3f05, 0xd6c1bc06, 0xc5914ff2, 0x37faccf1,
    0x69e9f0d5, 0x9b8273d6, 0x88d28022, 0x7ab90321, 0xae7367ca, 0x5c18e4c9, 0x4f48173d, 0xbd23943e,
    0xf36e6f75, 0x0105ec76, 0x12551f82, 0xe03e9c81, 0x34f4f86a, 0xc69f7b69, 0xd5cf889d, 0x27a40b9e,
    0x79b737ba, 0x8bdcb4b9, 0x988c474d, 0x6ae7c44e, 0xbe2da0a5, 0x4c4623a6, 0x5f16d052, 0xad7d5351};

template <>
uint64_t crc64::_uX2N[sizeof(crc64::_uX2N) / sizeof(crc64::_uX2N[0])] = {
    0x0080000000000000, 0x0000800000000000, 0x0000000080000000, 0x9a6c9329ac4bc9b5,
    0x10f4bb0f129310d6, 0x70f05dcea2ebd226, 0x311211205672822d, 0x2fc297db0f46c96e,
    0xca4d536fabf7da84, 0xfb4cdc3b379ee6ed, 0xea261148df25140a, 0x59ccb2c07aa6c9b4,
    0x20b3674a839af27a, 0x2d8e1986da94d583, 0x42cdf4c20337635d, 0x1d78724bf0f26839,
    0xb96c84e0afb34bd5, 0x5d2e1fcd2df0a3ea, 0xcd9506572332be42, 0x23bda2427f7d690f,
    0x347a953232374f07, 0x1c2a807ac2a8ceea, 0x9b92ad0e14fe1460, 0x2574114889f670b2,
    0x4a84a6c45e3bf520, 0x915bbac21cd1c7ff, 0xb0290ec579f291f5, 0xcf2548505c624e6e,
    0xb154f27bf08a8207, 0xce4e92344baf7d35, 0x51da8d7e057c5eb3, 0x9fb10823f5be15df,
    0x73b825b3ff1f71cf, 0x5db436c5406ebb74, 0xfa7ed8f3ec3f2bca, 0xc4d58efdc61b9ef6,
    0xa7e39e61e855bd45, 0x97ad46f9dd1bf2f1, 0x1a0abb01f853ee6b, 0x3f0827c3348f8215,
    0x4eb68c4506134607, 0x4a46f6de5df34e0a, 0x2d855d6a1c57a8dd, 0x8688da58e1115812,
    0x5232f417fc7c7300, 0xa4080fb2e767d8da, 0xd515a7e17693e562, 0x1181f7c862e94226,
    0x9e23cd058204ca91, 0x9b8992c57a0aed82, 0xb2c0afb84609b6ff, 0x2f7160553a5ea018,
    0x3cd378b5c99f2722, 0x814054ad61a3b058, 0xbf766189fce806d8, 0x85a5e898ac49f86f,
    0x34830d11bc84f346, 0x9644d95b173c8c1c, 0x150401ac9ac759b1, 0xebe1f7f46fb00eba,
    0x8ee4ce0c2e2bd662, 0x4000000000000000, 0x2000000000000000, 0x0800000000000000};

template <>
uint64_t crc64::_crc_table[sizeof(crc64::_crc_table) / sizeof(crc64::_crc_table[0])] = {
    0x0000000000000000, 0x7f6ef0c830358979, 0xfedde190606b12f2, 0x81b31158505e9b8b,
    0xc962e5739841b68f, 0xb60c15bba8743ff6, 0x37bf04e3f82aa47d, 0x48d1f42bc81f2d04,
    0xa61cecb46814fe75, 0xd9721c7c5821770c, 0x58c10d24087fec87, 0x27affdec384a65fe,
    0x6f7e09c7f05548fa, 0x1010f90fc060c183, 0x91a3e857903e5a08, 0xeecd189fa00bd371,
    0x78e0ff3b88be6f81, 0x078e0ff3b88be6f8, 0x863d1eabe8d57d73, 0xf953ee63d8e0f40a,
    0xb1821a4810ffd90e, 0xceecea8020ca5077, 0x4f5ffbd87094cbfc, 0x30310b1040a14285,
    0xdefc138fe0aa91f4, 0xa192e347d09f188d, 0x2021f21f80c18306, 0x5f4f02d7b0f40a7f,
    0x179ef6fc78eb277b, 0x68f0063448deae02, 0xe943176c18803589, 0x962de7a428b5bcf0,
    0xf1c1fe77117cdf02, 0x8eaf0ebf2149567b, 0x0f1c1fe77117cdf0, 0x7072ef2f41224489,
    0x38a31b04893d698d, 0x47cdebccb908e0f4, 0xc67efa94e9567b7f, 0xb9100a5cd963f206,
    0x57dd12c379682177, 0x28b3e20b495da80e, 0xa900f35319033385, 0xd66e039b2936bafc,
    0x9ebff7b0e12997f8, 0xe1d10778d11c1e81, 0x606216208142850a, 0x1f0ce6e8b1770c73,
    0x8921014c99c2b083, 0xf64ff184a9f739fa, 0x77fce0dcf9a9a271, 0x08921014c99c2b08,
    0x4043e43f0183060c, 0x3f2d14f731b68f75, 0xbe9e05af61e814fe, 0xc1f0f56751dd9d87,
    0x2f3dedf8f1d64ef6, 0x50531d30c1e3c78f, 0xd1e00c6891bd5c04, 0xae8efca0a188d57d,
    0xe65f088b6997f879, 0x9931f84359a27100, 0x1882e91b09fcea8b, 0x67ec19d339c963f2,
    0xd75adabd7a6e2d6f, 0xa8342a754a5ba416, 0x29873b2d1a053f9d, 0x56e9cbe52a30b6e4,
    0x1e383fcee22f9be0, 0x6156cf06d21a1299, 0xe0e5de5e82448912, 0x9f8b2e96b271006b,
    0x71463609127ad31a, 0x0e28c6c1224f5a63, 0x8f9bd7997211c1e8, 0xf0f5275142244891,
    0xb824d37a8a3b6595, 0xc74a23b2ba0eecec, 0x46f932eaea507767, 0x3997c222da65fe1e,
    0xafba2586f2d042ee, 0xd0d4d54ec2e5cb97, 0x5167c41692bb501c, 0x2e0934dea28ed965,
    0x66d8c0f56a91f461, 0x19b6303d5aa47d18, 0x980521650afae693, 0xe76bd1ad3acf6fea,
    0x09a6c9329ac4bc9b, 0x76c839faaaf135e2, 0xf77b28a2faafae69, 0x8815d86aca9a2710,
    0xc0c42c4102850a14, 0xbfaadc8932b0836d, 0x3e19cdd162ee18e6, 0x41773d1952db919f,
    0x269b24ca6b12f26d, 0x59f5d4025b277b14, 0xd846c55a0b79e09f, 0xa72835923b4c69e6,
    0xeff9c1b9f35344e2, 0x90973171c366cd9b, 0x1124202993385610, 0x6e4ad0e1a30ddf69,
    0x8087c87e03060c18, 0xffe938b633338561, 0x7e5a29ee636d1eea, 0x0134d92653589793,
    0x49e52d0d9b47ba97, 0x368bddc5ab7233ee, 0xb738cc9dfb2ca865, 0xc8563c55cb19211c,
    0x5e7bdbf1e3ac9dec, 0x21152b39d3991495, 0xa0a63a6183c78f1e, 0xdfc8caa9b3f20667,
    0x97193e827bed2b63, 0xe877ce4a4bd8a21a, 0x69c4df121b863991, 0x16aa2fda2bb3b0e8,
    0xf86737458bb86399, 0x8709c78dbb8deae0, 0x06bad6d5ebd3716b, 0x79d4261ddbe6f812,
    0x3105d23613f9d516, 0x4e6b22fe23cc5c6f, 0xcfd833a67392c7e4, 0xb0b6c36e43a74e9d,
    0x9a6c9329ac4bc9b5, 0xe50263e19c7e40cc, 0x64b172b9cc20db47, 0x1bdf8271fc15523e,
    0x530e765a340a7f3a, 0x2c608692043ff643, 0xadd397ca54616dc8, 0xd2bd67026454e4b1,
    0x3c707f9dc45f37c0, 0x431e8f55f46abeb9, 0xc2ad9e0da4342532, 0xbdc36ec59401ac4b,
    0xf5129aee5c1e814f, 0x8a7c6a266c2b0836, 0x0bcf7b7e3c7593bd, 0x74a18bb60c401ac4,
    0xe28c6c1224f5a634, 0x9de29cda14c02f4d, 0x1c518d82449eb4c6, 0x633f7d4a74ab3dbf,
    0x2bee8961bcb410bb, 0x548079a98c8199c2, 0xd53368f1dcdf0249, 0xaa5d9839ecea8b30,
    0x449080a64ce15841, 0x3bfe706e7cd4d138, 0xba4d61362c8a4ab3, 0xc52391fe1cbfc3ca,
    0x8df265d5d4a0eece, 0xf29c951de49567b7, 0x732f8445b4cbfc3c, 0x0c41748d84fe7545,
    0x6bad6d5ebd3716b7, 0x14c39d968d029fce, 0x95708ccedd5c0445, 0xea1e7c06ed698d3c,
    0xa2cf882d2576a038, 0xdda178e515432941, 0x5c1269bd451db2ca, 0x237c997575283bb3,
    0xcdb181ead523e8c2, 0xb2df7122e51661bb, 0x336c607ab548fa30, 0x4c0290b2857d7349,
    0x04d364994d625e4d, 0x7bbd94517d57d734, 0xfa0e85092d094cbf, 0x856075c11d3cc5c6,
    0x134d926535897936, 0x6c2362ad05bcf04f, 0xed9073f555e26bc4, 0x92fe833d65d7e2bd,
    0xda2f7716adc8cfb9, 0xa54187de9dfd46c0, 0x24f29686cda3dd4b, 0x5b9c664efd965432,
    0xb5517ed15d9d8743, 0xca3f8e196da80e3a, 0x4b8c9f413df695b1, 0x34e26f890dc31cc8,
    0x7c339ba2c5dc31cc, 0x035d6b6af5e9b8b5, 0x82ee7a32a5b7233e, 0xfd808afa9582aa47,
    0x4d364994d625e4da, 0x3258b95ce6106da3, 0xb3eba804b64ef628, 0xcc8558cc867b7f51,
    0x8454ace74e645255, 0xfb3a5c2f7e51db2c, 0x7a894d772e0f40a7, 0x05e7bdbf1e3ac9de,
    0xeb2aa520be311aaf, 0x944455e88e0493d6, 0x15f744b0de5a085d, 0x6a99b478ee6f8124,
    0x224840532670ac20, 0x5d26b09b16452559, 0xdc95a1c3461bbed2, 0xa3fb510b762e37ab,
    0x35d6b6af5e9b8b5b, 0x4ab846676eae0222, 0xcb0b573f3ef099a9, 0xb465a7f70ec510d0,
    0xfcb453dcc6da3dd4, 0x83daa314f6efb4ad, 0x0269b24ca6b12f26, 0x7d0742849684a65f,
    0x93ca5a1b368f752e, 0xeca4aad306bafc57, 0x6d17bb8b56e467dc, 0x12794b4366d1eea5,
    0x5aa8bf68aecec3a1, 0x25c64fa09efb4ad8, 0xa4755ef8cea5d153, 0xdb1bae30fe90582a,
    0xbcf7b7e3c7593bd8, 0xc399472bf76cb2a1, 0x422a5673a732292a, 0x3d44a6bb9707a053,
    0x759552905f188d57, 0x0afba2586f2d042e, 0x8b48b3003f739fa5, 0xf42643c80f4616dc,
    0x1aeb5b57af4dc5ad, 0x6585ab9f9f784cd4, 0xe436bac7cf26d75f, 0x9b584a0fff135e26,
    0xd389be24370c7322, 0xace74eec0739fa5b, 0x2d545fb4576761d0, 0x523aaf7c6752e8a9,
    0xc41748d84fe75459, 0xbb79b8107fd2dd20, 0x3acaa9482f8c46ab, 0x45a459801fb9cfd2,
    0x0d75adabd7a6e2d6, 0x721b5d63e7936baf, 0xf3a84c3bb7cdf024, 0x8cc6bcf387f8795d,
    0x620ba46c27f3aa2c, 0x1d6554a417c62355, 0x9cd645fc4798b8de, 0xe3b8b53477ad31a7,
    0xab69411fbfb21ca3, 0xd407b1d78f8795da, 0x55b4a08fdfd90e51, 0x2ada5047efec8728};

#undef crc32_POLY
#undef crc64_POLY
#undef BIT64
#undef BIT32
}
}

namespace dsn {
namespace utils {
uint32_t crc32_calc(const void *ptr, size_t size, uint32_t init_crc)
{
    return dsn::utils::crc32::compute(ptr, size, init_crc);
}

uint32_t crc32_concat(uint32_t xy_init,
                      uint32_t x_init,
                      uint32_t x_final,
                      size_t x_size,
                      uint32_t y_init,
                      uint32_t y_final,
                      size_t y_size)
{
    return dsn::utils::crc32::concatenate(
        0, x_init, x_final, (uint64_t)x_size, y_init, y_final, (uint64_t)y_size);
}

uint64_t crc64_calc(const void *ptr, size_t size, uint64_t init_crc)
{
    return dsn::utils::crc64::compute(ptr, size, init_crc);
}

uint64_t crc64_concat(uint32_t xy_init,
                      uint64_t x_init,
                      uint64_t x_final,
                      size_t x_size,
                      uint64_t y_init,
                      uint64_t y_final,
                      size_t y_size)
{
    return ::dsn::utils::crc64::concatenate(
        0, x_init, x_final, (uint64_t)x_size, y_init, y_final, (uint64_t)y_size);
}
}
}
