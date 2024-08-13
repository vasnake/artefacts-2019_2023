/** Created by vasnake@gmail.com on 2024-08-13
  */
package com.github.vasnake.spark.udf.`java-api`

//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class MurmurHash3_32UDFTest extends AnyFlatSpec with should.Matchers {
  val udf = new MurmurHash3_32UDF()

  it should "compute reference values" in {
    // testOnly *MurmurHash3_32* -- -z "reference"
    assert(udf.call("SOCIAL") === "3625832686")
    assert(udf.call("DATING") === "1545499902")

    /*

That's why hash values are as you see it above in test:

Python2, string as bytes
>>> from sklearn.utils import murmurhash3_32
>>> str(murmurhash3_32("SOCIAL", positive=True))
'3625832686'
>>> str(murmurhash3_32("DATING", positive=True))
'1545499902'

>>> str(murmurhash3_32("SOCIAL", seed=0, positive=False))
'-669134610'
>>> str(murmurhash3_32("SOCIAL", seed=0, positive=True))
'3625832686'

"{0:032b}".format(3625832686) # 11011000000111011101000011101110
"{0:032b}".format(-669134610) # -0100111111000100010111100010010

'SOCIAL' bytes: [83,79,67,73,65,76]

https://github.com/scikit-learn/scikit-learn/blob/e0ebc7839153da72e091f385ee4e6d4df51f96ef/sklearn/utils/murmurhash.pyx#L80
cpdef np.uint32_t murmurhash3_bytes_u32(bytes key, unsigned int seed):
    cdef np.uint32_t out
    MurmurHash3_x86_32(<char*> key, len(key), seed, &out)
    return out

https://github.com/scikit-learn/scikit-learn/blob/114616d9f6ce9eba7c1aacd3d4a254f868010e25/sklearn/utils/src/MurmurHash3.cpp#L105
cdef extern from "src/MurmurHash3.h":
    void MurmurHash3_x86_32(void *key, int len, np.uint32_t seed, void *out)

     */

  }
}
