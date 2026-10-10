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

  // donat un llibre en text pla (string) retorna el mateix text sense els caràcters que no fan paraules, en minúscula
  def normalitzar(textFitxer: String): List[String] = {
    val textFitxerMinus = textFitxer.toLowerCase() // passem totes les lletres a mínuscules

    val textFitxerLletres = textFitxerMinus.replaceAll("[^a-z\\s]", " ") // eliminem tot els caràcters que no fan paraules

    val textFitxerNorm = textFitxerLletres.replaceAll("\\s+", " ") // eliminem els espais en blanc sobrants deixant només un espai en blanc

    textFitxerNorm.trim().split(" ").toList
  }

  def llistaFreqs(listTotalWords: List[String]): List[(String, Int)] = {
    def comptaFreq(word: String, acum: List[(String, Int)]): List[(String, Int)] = {
      acum match
        case Nil => (word, 1) :: Nil
        case cap :: cua => if cap._1 == word then (cap._1, cap._2 + 1) :: cua else cap :: comptaFreq(word, cua)
    }

    var listCountWords: List[(String, Int)] = Nil

    // Comptar ocurrencies de paraules
    for word <- listTotalWords do listCountWords = comptaFreq(word, listCountWords)

    // Finalment, retornem el resultat (la llista de tuples (String, Int))
    listCountWords
  }

  def mostrarResults(listTotalWords: List[String], listCountWords: List[(String, Int)]): Unit = {
    val listOrd = quicksortf(listCountWords.toArray).take(10) // ordena la llista listCountWords i obtenim els deu primers
    println(s"Num de paraules: ${listTotalWords.length}\tDiferents: ${listCountWords.length}")
    println("Paraules\tocurrencies\tfrequencia")
    println("---------------------------------------")
    for f <- listOrd do
      // calcula el percentatge i mostra la fila X
      val percentage = (f._2.toDouble / listTotalWords.length) * 100
      println(s"${f._1}\t${f._2}\t${String.format("%.2f", percentage).replace(".", ",")}")
  }

  // donat un llibre en text pla (string) retorna una llista de tuples de paraules i el número d'aparicions
  def freq(textFitxer: String): List[(String, Int)] = {
    // Primer pas, normalitzar a un String format de paraules en minúscules separades per espais
    val listTotalWords = normalitzar(textFitxer)

    // Segon pas, obtenir la llista de frequències
    val listCountWords = llistaFreqs(listTotalWords)

    // Tercer pas, mostrar resultats
    mostrarResults(listTotalWords, listCountWords)

    listCountWords
  }

  def nonstopfreq(textFitxer: String, llistaStopWords: List[String]): List[(String, Int)] = {
    def treuNonStopWords(listWords: List[String], stopWords: List[String], acum: List[String]): List[String] = {
      listWords match
        case Nil => acum
        case cap :: cua => if stopWords `contains` cap then treuNonStopWords(cua, stopWords, acum) else treuNonStopWords(cua, stopWords, cap :: acum)
    }

    // Primer pas, normalitzar a un String format de paraules en minúscules separades per espais
    val listTotalWords = normalitzar(textFitxer)

    // Segon pas, eliminem els stop-words de la llista de totes les paraules
    val listSenseStopsWords = treuNonStopWords(listTotalWords, llistaStopWords, Nil)

    // Tercer pas, obtenir la llista de frequències
    val listCountWords = llistaFreqs(listSenseStopsWords)

    // Quart pas, mostrar resultats
    mostrarResults(listSenseStopsWords, listCountWords)

    listCountWords
  }

  def paraulafreqfreq(textFitxer: String): Unit = {
    // Primer pas, normalitzar a un String format de paraules en minúscules separades per espais
    val listTotalWords = normalitzar(textFitxer)
    // I obtenir la llista de frequències
    val listCountWords = llistaFreqs(listTotalWords)

    println(listCountWords)
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
  val listStopWords = ProcessListStrings.llegirFitxer("assets/simil/stopwords/english-stop.txt").replaceAll("'", "\n").split("\n")
  val freqsWordsNonStop = Simil.nonstopfreq(textFitxer1, listStopWords.toList)

  // Distribució de paraules
  paraulafreqfreq(textFitxer1)

  // ngrames

  // Vector space model
}
