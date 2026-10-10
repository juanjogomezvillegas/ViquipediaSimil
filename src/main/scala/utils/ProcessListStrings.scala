package edu.udg.pda
package utils

import org.apache.commons.io.{FileUtils, IOUtils}
import java.io.File
import java.nio.charset.StandardCharsets

object ProcessListStrings {
  
  // donat el nom d'un fitxer en retornem el seu contingut com a String
  def llegirFitxer(fitxer: String): String = {
    val initialFile = new File(fitxer)
    val targetStream = FileUtils.openInputStream(initialFile)
    val textString = IOUtils.toString(targetStream, StandardCharsets.UTF_8)
    targetStream.close()
    textString
  }

  // donat un nom de directori retornem la llista de fitxers que hi ha
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  // donat un nom de directori mostrem els fitxers txt d'un directori
  def mostrarTextDirectori(directori: String): Unit = {
    val fitxerstxt = for (f <- getListOfFiles(directori); nom = f.getName if nom.takeRight(3) == "txt")
      yield nom
    for (f <- fitxerstxt) {
      println(f)
      println(ProcessListStrings.llegirFitxer(directori ++ "/" ++ f))
      println("--------------------------------------------------")
    }
  }

}