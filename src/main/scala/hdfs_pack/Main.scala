package hdfs_pack

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.net.URI

object Main extends App {

  val fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration())

  val dstPath = new Path("/ods")
  createFolder(dstPath)

  try {
        fs.listStatus(new Path("/stage")).foreach(
            x => {
              println("moving directories " + x.getPath.getName + " to " + dstPath)
              val ok = fs.rename(x.getPath, dstPath)
                                        if (ok) {
                                           println("moved " + x.getPath.getName)
                                        } else {
                                          println("Error! Not moved " + x.getPath.getName)
                                        }
            }
          )
          fs.listStatus(dstPath).foreach(
                x => {
                        val fileList = fs.listStatus(x.getPath)
                            .filter(y => y.getPath.getName.contains("csv"))

                        val resultFile = new Path(x.getPath, fileList(0).getPath.getName)
                        val restFiles: Array[Path] = fileList.map(f => f.getPath).tail

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
