package edu.udg.pda
package utils

object Simil extends App {
  // donat un llibre en text pla (string) retorna el mateix text sense els caràcters que no fan paraules, en minúscula
  def normalitzar(textFitxer: String): String = {
    val textFitxerMinus = textFitxer.toLowerCase() // passem totes les lletres a mínuscules

    val textFitxerLletres = textFitxerMinus.replaceAll("[^a-z\\s]", " ") // eliminem tot els caràcters que no fan paraules

    val textFitxerNorm = textFitxerLletres.replaceAll("\\s+", " ") // eliminem els espais en blanc sobrants deixant només un espai en blanc

    textFitxerNorm.trim()
  }

  // donat un llibre en text pla (string) retorna una llista de tuples de paraules i el número d'aparicions
  def freq(textFitxer: String): List[String] = { // List[(String, Int)] = {
    /*def comptaFreq(listWords: List[String], acum: List[(String, Int)]): List[(String, Int)] = {
      listWords match
        case Nil => acum
        case cap :: cua => if acum `exists` (t => t._1 == cap) then {
          acum `map` {
            case x => x
            case (word, count) :: resta => if word == cap then (word, count + 1) }
        } else (cap, 1) :: acum
    }*/

    // Primer pas, normalitzar a un String format de paraules en minúscules separades per espais
    val textFitxerNorm = normalitzar(textFitxer)

    val listTotalWords = textFitxerNorm.split(" ")

    listTotalWords.toList
  }

  // Fixem els dos fitxes per fer proves
  val documentsDir = "assets/simil/documents"
  val fitxer1 = documentsDir + "/pg2500.txt"
  val fitxer2 = documentsDir + "/pg74.txt"

  // Llegim el contingut dels dos fitxers com a text pla
  val textFitxer1 = ProcessListStrings.llegirFitxer(fitxer1)
  val textFitxer2 = ProcessListStrings.llegirFitxer(fitxer2)

  println(Simil.freq(textFitxer1))

  // Freqüències de paraules

  // Sense stop-words

  // Distribució de paraules

  // ngrames

  // Vector space model
}
