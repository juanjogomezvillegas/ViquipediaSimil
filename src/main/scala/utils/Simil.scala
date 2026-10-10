package edu.udg.pda
package utils

object Simil extends App {
  // mètode quicksort per ordenar de més gran a més petit un list de tuples de (String, Int), només per mostrar els resultats
  def quicksortf(xs: Array[(String, Int)]): Array[(String, Int)] =
    if xs.length <= 1 then xs
    else
      val pivot: (String, Int) = xs(xs.length / 2)
      Array.concat(
        quicksortf(xs filter (x => pivot._2 < x._2)),
        xs filter (x => x._2 == pivot._2),
        quicksortf(xs filter (x => pivot._2 > x._2))
      )

  // donat un num i un total, calcula el tant per cent
  def calculTantPerCent(num: Int, total: Int): Double = (num.toDouble / total) * 100

  // donat un llibre en text pla (string) retorna el mateix text sense els caràcters que no fan paraules, en minúscula
  def normalitzar(textFitxer: String): String = {
    val textFitxerMinus = textFitxer.toLowerCase() // passem totes les lletres a mínuscules

    val textFitxerLletres = textFitxerMinus.replaceAll("[^a-z\\s]", " ") // eliminem tot els caràcters que no fan paraules

    val textFitxerNorm = textFitxerLletres.replaceAll("\\s+", " ") // eliminem els espais en blanc sobrants deixant només un espai en blanc

    textFitxerNorm.trim()
  }

  // donat un llibre en text pla (string) retorna una llista de tuples de paraules i el número d'aparicions
  def freq(textFitxer: String): List[(String, Int)] = {
    def comptaFreq(word: String, acum: List[(String, Int)]): List[(String, Int)] = {
      acum match
        case Nil => (word, 1) :: Nil
        case cap :: cua => if cap._1 == word then (cap._1, cap._2 + 1) :: cua else cap :: comptaFreq(word, cua)
    }

    // Primer pas, normalitzar a un String format de paraules en minúscules separades per espais
    val textFitxerNorm = normalitzar(textFitxer)

    val listTotalWords = textFitxerNorm.split(" ")
    var listCountWords: List[(String, Int)] = Nil

    // Segon pas, comptar ocurrencies de paraules
    for word <- listTotalWords do listCountWords = comptaFreq(word, listCountWords)

    // Tercer pas, mostrar els resultats
    val listOrd = quicksortf(listCountWords.toArray).take(10) // ordena la llista listCountWords i obtenim els deu primers
    println(s"Num de paraules: ${listTotalWords.length}\tDiferents: ${listCountWords.length}")
    println("Paraules\tocurrencies\tfrequencia")
    println("---------------------------------------")
    for f <- listOrd do
      // calcula el percentatge i mostra la fila X
      val percentage = calculTantPerCent(f._2, listTotalWords.length)
      println(s"${f._1}\t${f._2}\t${String.format("%.2f", percentage).replace(".", ",")}")

    // Finalment, retornem el resultat (la llista de tuples (String, Int))
    listCountWords
  }

  // Fixem els dos fitxes per fer proves
  val documentsDir = "assets/simil/documents"
  val fitxer1 = documentsDir + "/pg11.txt"
  val fitxer2 = documentsDir + "/pg74.txt"

  // Llegim el contingut dels dos fitxers com a text pla
  val textFitxer1 = ProcessListStrings.llegirFitxer(fitxer1)
  val textFitxer2 = ProcessListStrings.llegirFitxer(fitxer2)

  // Freqüències de paraules
  val freqsWords = Simil.freq(textFitxer1)

  // Sense stop-words


  // Distribució de paraules

  // ngrames

  // Vector space model
}
