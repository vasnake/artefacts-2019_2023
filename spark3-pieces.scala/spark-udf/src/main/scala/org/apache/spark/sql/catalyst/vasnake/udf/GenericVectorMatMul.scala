/** Created by vasnake@gmail.com on 2024-07-19
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import java.lang.{ Double => jDouble }

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.vasnake.udf.base.GenericBinaryArraysElements

/** Returns an array of elements produced by multiplication of two matrices or multiplication of matrix to vector,
  * depending on the shape of arguments.
  * Registration: `functions.registerAs("generic_matmul", "matmul", spark, overrideIfExists = true)`
  */
@ExpressionDescription(
  usage = """
  _FUNC_(array1, array2) - Returns an array of elements where `C(i,j) = A(i) dot B(j)`.
  Only square matrices are supported, matrix rows concatenated to form an input array.
  Second argument could be a matrix or a vector, in that case vector.length == sqrt(matrix.size)
  """,
  examples = """
    Examples:
    matrix * matrix:
      > SELECT _FUNC_(array(1, 2, 3, 4), array(5, 6, 7, 8));
       [19, 22, 43, 50]
    matrix * vector:
      > SELECT _FUNC_(array(1, 2, 3, 4), array(5, 6));
       [17, 39]
  """,
  since = "0.1.0"
)
case class GenericVectorMatMul(left: Expression, right: Expression)
    extends GenericBinaryArraysElements {
  def binaryOp(x1: jDouble, x2: jDouble): jDouble =
    throw new IllegalAccessError("This method eliminated by optimized class structure")

  override protected def nullSafeEvalImplBase(array1: ArrayData, array2: ArrayData): ArrayData =
    throw new IllegalAccessError("This method eliminated by optimized class structure")

  override def nullSafeEval(value1: Any, value2: Any): Any = {
    // Called from `eval` when both arguments are not null
    val array1 = value1.asInstanceOf[ArrayData]
    val array2 = value2.asInstanceOf[ArrayData]

    // Size checks and function selection could be optimized using `lazy val`,
    // but only if you can guarantee that all rows have vectors with the same shape.
    if (array1.numElements() == 0 && array2.numElements() == 0)
      new GenericArrayData(new Array[Any](0))
    else if (array1.numElements() == array2.numElements()) { // matrices?
      val size = math.round(math.sqrt(array1.numElements()))
      if (size * size == array1.numElements()) matmat(array1, array2, size.toInt) // yes, matrices
      else null // wrong mat*mat shape
    } // end of matrices branch.
    else if ((array2.numElements() * array2.numElements()) == array1.numElements())
      matvec(array1, array2)
    else null // wrong mat*vec shape
  }

  private def matmat(
    matA: ArrayData,
    matB: ArrayData,
    size: Int
  ): ArrayData = {
    val res = new Array[Any](matA.numElements())

    // Could be optimized, don't use Range object, use simple loops.
    // Doesn't have to be n^3 complexity.
    (0 until size).foreach { rowIdx =>
      (0 until size).foreach(colIdx =>
        res.update((rowIdx * size) + colIdx, dot(matA, matB, rowIdx, colIdx, size))
      )
    }

    new GenericArrayData(res)
  }

  private def matvec(mat: ArrayData, vec: ArrayData): ArrayData = {
    val res = new Array[Any](vec.numElements())

    // Could be optimized
    res.indices.foreach(rowIdx => res.update(rowIdx, dot(mat, rowIdx, vec)))

    new GenericArrayData(res)
  }

  @inline
  private def dot(
    matA: ArrayData,
    matB: ArrayData,
    rowIdx: Int,
    colIdx: Int,
    size: Int
  ): Any = {
    var res: jDouble = 0
    var i: Int = 0

    while (res != null && i < size) {
      res = safeDotAcc(
        matA.get((rowIdx * size) + i, elementType),
        matB.get(colIdx + (size * i), elementType),
        res
      )
      i += 1
    }

    if (res == null) null else toElementType(res)
  }

  @inline
  private def dot(
    mat: ArrayData,
    matRowIdx: Int,
    vec: ArrayData
  ): Any = {
    val startIdx = matRowIdx * vec.numElements()
    // def unsafe = vec.indices.map(i => mat(startIdx + i) * vec(i)).sum

    var res: jDouble = 0
    var i: Int = 0

    while (res != null && i < vec.numElements()) {
      res = safeDotAcc(
        mat.get(startIdx + i, elementType),
        vec.get(i, elementType),
        res
      )
      i += 1
    }

    if (res == null) null else toElementType(res)
  }

  @inline
  private def safeDotAcc(
    _a: Any,
    _b: Any,
    acc: jDouble
  ): jDouble = (_a, _b) match { // optimizations needed
    case (a, b) if a == null || b == null => null // acc check must be in caller
    case (a, b) =>
      val x1 = toDouble(a)
      val x2 = toDouble(b)
      if (x1.isNaN || x1.isInfinite || x2.isNaN || x2.isInfinite) null // Boxing, could be optimized
      else acc + (x1 * x2)
  }
}
/*

mat. mul. and dot product cheatsheet

mat * mat => mat
mat * vec => vec
vec * vec => number

mat * mat => mat

C = A * B
c(i,j) = a(i) dot b(j) # vectors dot product
i: row
j: column

count(A cols) must be = count(B rows)
count(C rows) = count(A rows)
count(C cols) = count(B cols)

# happy path, mat * mat
matmul([1,2,3,4], [5,6,7,8]) == [19, 22, 43, 50]
1,2   5,6   c11, c12 = [1,2]*[5,7], [1,2]*[6,8] = 1*5+2*7, 1*6+2*8 = 19, 22
3,4   7,8   c21, c22 = [3,4]*[5,7], [3,4]*[6,8] = 3*5+4*7, 3*6+4*8 = 43, 50

# happy path, mat * vec
matmul([1,2,3,4], [5,6]) == [17, 39]
1,2   5   x=[1,2]*[5,6] = 1*5 + 2*6 = 17
3,4   6   y=[3,4]*[5,6] = 3*5 + 4*6 = 39

 */
