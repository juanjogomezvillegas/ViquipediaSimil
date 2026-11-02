package edu.udg.pda
package utils

import scala.util.matching.Regex
import scala.xml.{Elem, XML}


object ViquipediaParse extends App {

  // Definim una case class per a retornar diversos valor, el titol de la pàgina, el contingut i les referències trobades.
  // El contingut, s'ha de polir més? treure refs? stopwords?...
  case class ResultViquipediaParsing(titol: String, contingut: String, refs: List[String])


  // Metode que parseja contingüt d'un document
  def parseViquipediaFile(filename: String): ResultViquipediaParsing = {
    val xmlleg = new java.io.InputStreamReader(new java.io.FileInputStream(filename), "UTF-8")

    // Agafo el document XML i ja està internament estructurat per anar accedint als camps que volguem
    val xmllegg: Elem = XML.load(xmlleg)

    // obtinc el titol
    val titol = (xmllegg \\ "title").text

    // obtinc el contingut de la pàgina
    val contingut = (xmllegg \\ "text").text

    // identifico referències
    val ref = new Regex("\\[\\[[^\\]]*\\]\\]")
    //println("La pagina es: " + titol)
    //println("i el contingut: ")
    //println(contingut)
    val refs = (ref findAllIn contingut).toList

    // elimino les que tenen :
    val filteredRefs = refs.filterNot(_.contains(':'))

    // caldrà eliminar-ne més?

    //for (r <- refs) println(r)
    //println(refs.length)
    //println(filteredRefs.length)
    xmlleg.close()
    ResultViquipediaParsing(titol, contingut, filteredRefs)
  }

  // Fixem el fitxer xml que volem tractar a l'exemple
  val documentsDir = "assets/viquipedia/documents"
  val exampleFilename = documentsDir + "/32509.xml"

  def testParse: ResultViquipediaParsing = parseViquipediaFile(exampleFilename)

  //println(testParse)
}