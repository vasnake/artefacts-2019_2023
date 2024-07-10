/** Created by vasnake@gmail.com on 2024-07-08
  */
package com.github.vasnake.common.file

import java.io.File
import java.net.URI

import org.apache.commons.io.FilenameUtils

// TODO: add docstrings, tests

object FileToolbox {

  /** Read local text file using UTF-8 encoding.
    * @param path path to local text file
    * @return file contents as lines joined with '\n'
    */
  def loadTextFile(path: String): String = {
    val src = scala.io.Source.fromFile(path, "UTF-8")
    val res = src.getLines.mkString("\n")
    src.close()

    res
  }

  /** Split path by os.path.separator and return dirname from 'dirname/filename' pair.
    * @see org.apache.commons.io.FilenameUtils.getFullPathNoEndSeparator
    *
    * @param path path to file, e.g. /tmp/foo/bar/fname.txt
    * @return first part from pair 'dirname/filename' e.g. '/tmp/foo/bar'
    */
  def getPathDirname(path: String): String =
    FilenameUtils.getFullPathNoEndSeparator(path)

  /** Split path by os.path.separator and returns filename from 'dirname/filename' pair.
    * @see org.apache.commons.io.FilenameUtils.getName
    *
    * @param path path to file, e.g. /tmp/foo/bar/fname.txt
    * @return second part from pair 'dirname/filename' e.g. 'fname.txt'
    */
  def getPathBasename(path: String): String =
    FilenameUtils.getName(path)

  /** Concatenate (dirname, os.path.separator, filename).
    * @see org.apache.commons.io.FilenameUtils.concat
    *
    * @param dirname first part of the path
    * @param filename second part of the path
    * @return path to file
    */
  def joinPath(dirname: String, filename: String): String =
    FilenameUtils.concat(dirname, filename)

  /** Shortcut for obj.getClass.getResource(name).toURI
    * @param obj any object
    * @param name resource name
    * @return uri to resource for given class
    */
  def getResourceURI(obj: Any, name: String): URI = obj.getClass.getResource(name).toURI

  /** Shortcut for obj.getClass.getResource(name).toURI.getRawPath
    * @param obj any object
    * @param name resource name
    * @return path to resource for given class
    */
  def getResourcePath(obj: Any, name: String): String = getResourceURI(obj, name).getRawPath

  def getAbsolutePath(relativePath: String): String =
    (new File(relativePath)).getAbsolutePath

  def makeDirs(path: String): Boolean =
    (new File(path)).mkdirs()

  private val dont_forget_to_drop_this_HDFSFileToolbox =
    """

object HDFSFileToolbox {
  import org.apache.spark.sql.SparkSession

  /**
    * Read text from `path` using sparkContext.textFile.
    * Obtain spark session using `SparkSession.builder().getOrCreate()`.
    *
    * @param path path to text file on hdfs (or local disk)
    * @return text combined from lines separated with '\n'
    */
  def loadTextFile(path: String): String = {
    val  spark: SparkSession = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext

    // sort lines by index in attempt to preserve lines order in file
    val lines = sc.textFile(path, minPartitions = 1)
      .zipWithIndex
      .collect
      .sortBy(_._2).map(_._1)

    lines.mkString("\n")
  }

}

      """
}
