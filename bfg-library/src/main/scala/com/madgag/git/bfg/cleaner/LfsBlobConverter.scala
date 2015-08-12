/*
 * Copyright (c) 2015 Roberto Tyley
 *
 * This file is part of 'BFG Repo-Cleaner' - a tool for removing large
 * or troublesome blobs from Git repositories.
 *
 * BFG Repo-Cleaner is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * BFG Repo-Cleaner is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/ .
 */

package com.madgag.git.bfg.cleaner

import java.nio.charset.Charset
import java.security.{DigestOutputStream, DigestInputStream, MessageDigest}

import com.google.common.io.ByteStreams
import com.madgag.git._
import com.madgag.git.bfg.model.{FileName, RegularFile, TreeBlobEntry, TreeBlobs}
import com.madgag.textmatching.{Glob, TextMatcher}
import org.apache.commons.codec.binary.Hex.encodeHexString
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.lib.Constants.OBJ_BLOB
import org.eclipse.jgit.lib.{ObjectId, ObjectLoader}

import scala.util.Try
import scalax.file.ImplicitConversions._
import scalax.file.Path
import scalax.file.Path.createTempFile
import scalax.io.JavaConverters._
import scalax.io.Resource

class LfsBlobConverter(
  lfsGlobExpression: String,
  repo: FileRepository
) extends TreeBlobModifier {

  val lfsObjectsDir = repo.getDirectory / "lfs" / "objects"

  val threadLocalObjectDBResources = repo.getObjectDatabase.threadLocalResources

  val lfsGlob = TextMatcher(Glob, lfsGlobExpression)

  val lfsSuitableFiles: (FileName => Boolean) = f => lfsGlob(f.string)

  val charset = Charset.forName("UTF-8")

  val GitAttributesFileName = FileName(".gitattributes")

  val gitAttributesLine = s"$lfsGlobExpression filter=lfs diff=lfs merge=lfs -text"

  override def apply(dirtyBlobs: TreeBlobs) = {
    val cleanedBlobs = super.apply(dirtyBlobs)
    if (cleanedBlobs == dirtyBlobs) cleanedBlobs else {
      val newGitAttributesId = cleanedBlobs.entryMap.get(GitAttributesFileName).fold {
        storeBlob(gitAttributesLine)
      } {
        case (_, oldGitAttributesId) =>
          val objectLoader = threadLocalObjectDBResources.reader().open(oldGitAttributesId)
          val oldAttributes = objectLoader.getBytes.asInput.lines().toSeq

          if (oldAttributes.contains(gitAttributesLine)) oldGitAttributesId else {
            storeBlob((oldAttributes :+ gitAttributesLine).mkString("\n"))
          }
      }
      cleanedBlobs.copy(entryMap = cleanedBlobs.entryMap + (GitAttributesFileName -> (RegularFile, newGitAttributesId)))
    }
  }

  override def fix(entry: TreeBlobEntry) = {
    val oid = (for {
      _ <- Some(entry.filename) filter lfsSuitableFiles
      loader = threadLocalObjectDBResources.reader().open(entry.objectId)
      (shaHex, lfsPath) <- buildLfsFileFrom(loader)
    } yield {
      val pointer =
        s"""|version https://git-lfs.github.com/spec/v1
            |oid sha256:$shaHex
            |size ${loader.getSize}
            |""".stripMargin
  
      storeBlob(pointer)
    }).getOrElse(entry.objectId)

    (entry.mode, oid)
  }

  def storeBlob(blobText: String): ObjectId = {
    threadLocalObjectDBResources.inserter().insert(OBJ_BLOB, blobText.getBytes(charset))
  }

  def buildLfsFileFrom(loader: ObjectLoader): Option[(String, Path)] = {
    val tmpFile = createTempFile()
    println(s"${loader.getSize} going to $tmpFile")

    val digest = MessageDigest.getInstance("SHA-256")

    // use loader.copyTo() instead?

    for {
      outStream <- tmpFile.outputStream()
    } loader.copyTo(new DigestOutputStream(outStream, digest))

    println(s"${loader.getSize} copied to $tmpFile")

    val shaHex = encodeHexString(digest.digest())

    val lfsPath = lfsObjectsDir / shaHex.substring(0, 2) / shaHex.substring(2, 4) / shaHex

    val ensureLfsFile = Try(if (!lfsPath.exists) tmpFile moveTo lfsPath).recover {
      case _ => lfsPath.size.contains(loader.getSize)
    }

    Try(tmpFile.delete(force = true))

    println(s"${loader.getSize} copied to $shaHex")

    for (_ <- ensureLfsFile.toOption) yield shaHex -> lfsPath
  }
}