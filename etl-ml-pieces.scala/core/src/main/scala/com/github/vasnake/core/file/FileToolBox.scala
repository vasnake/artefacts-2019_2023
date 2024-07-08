/**
 * Created by vasnake@gmail.com on 2024-07-08
 */
package com.github.vasnake.core.file

// TODO: remove dependency (or move module from core)
import org.apache.commons.io.FilenameUtils

import java.io.File
import java.net.URI

// TODO: add tests

object FileToolbox {

  /**
   * Read local text file using UTF-8 encoding.
   * @param path path to local text file
   * @return file contents as lines joined with '\n'
   */
  def loadTextFile(path: String): String = {
    val src = scala.io.Source.fromFile(path, "UTF-8")
    val res = src.getLines.mkString("\n")
    src.close()

    res
  }

  /**
   * Split path by os.path.separator and return dirname from 'dirname/filename' pair.
   * @see org.apache.commons.io.FilenameUtils.getFullPathNoEndSeparator
   *
   * @param path path to file, e.g. /tmp/foo/bar/fname.txt
   * @return first part from pair 'dirname/filename' e.g. '/tmp/foo/bar'
   */
  def getPathDirname(path: String): String = {
    FilenameUtils.getFullPathNoEndSeparator(path)
  }

  /**
   * Split path by os.path.separator and returns filename from 'dirname/filename' pair.
   * @see org.apache.commons.io.FilenameUtils.getName
   *
   * @param path path to file, e.g. /tmp/foo/bar/fname.txt
   * @return second part from pair 'dirname/filename' e.g. 'fname.txt'
   */
  def getPathBasename(path: String): String = {
    FilenameUtils.getName(path)
  }

  /**
   * Concatenate (dirname, os.path.separator, filename).
   * @see org.apache.commons.io.FilenameUtils.concat
   *
   * @param dirname first part of the path
   * @param filename second part of the path
   * @return path to file
   */
  def joinPath(dirname: String, filename: String): String = {
    FilenameUtils.concat(dirname, filename)
  }

  /**
   * Shortcut for obj.getClass.getResource(name).toURI
   * @param obj any object
   * @param name resource name
   * @return uri to resource for given class
   */
  def getResourceURI(obj: Any, name: String): URI = obj.getClass.getResource(name).toURI

  /**
   * Shortcut for obj.getClass.getResource(name).toURI.getRawPath
   * @param obj any object
   * @param name resource name
   * @return path to resource for given class
   */
  def getResourcePath(obj: Any, name: String): String = getResourceURI(obj, name).getRawPath

  def getAbsolutePath(relpath: String): String = {
    new File(relpath).getAbsolutePath
  }
}
