package hdfs_pack

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.net.URI

object Main extends App {

  val fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration())

  val srcPath = new Path("/stage")

  val dstPath = new Path("/ods")
  createFolder(dstPath)

  try {
    fs.listStatus(srcPath).foreach(
      x => {
              val fileList = fs.listStatus(x.getPath)
                            .filter(y => y.getPath.getName.contains("csv"))

              println("folder contains files .csv")
                    fileList.foreach(x => println(x.getPath.getName))

              val destDir = new Path(dstPath, x.getPath.getName)
              createFolder(destDir)

              println("moving .csv files to " + destDir)
              fileList.foreach(fl => {
                                        val ok = fs.rename(new Path(x.getPath, fl.getPath.getName), destDir)
                                        if (ok) {
                                           println("moved " + fl.getPath.getName)
                                        }
                                     }
                              )

              println("deleting " + x.getPath)
              val ok = fs.delete(x.getPath, true)
              if (ok) {
                 println("deleted")
              }
      }
    )

    fs.listStatus(dstPath).foreach(
      x => {
              val fileList = fs.listStatus(x.getPath)
              val resultFile = new Path(x.getPath, fileList(0).getPath.getName)
              val restFiles: Array[Path] = fileList.map(fs => fs.getPath).tail

              println("adding all files into " + resultFile)
              if (restFiles.size > 0) {
                fs.concat(resultFile, restFiles)
              }
      }
    )

  } catch {
    case e: Exception => println(e.getMessage)
  } finally {
    fs.close()
  }

  def createFolder(folderPath: Path): Unit = {
    if (!fs.exists(folderPath)) {
      fs.mkdirs(folderPath)
      println("created " + folderPath)
    }
  }
}
