package edu.udg.pda
package utils


object Simil extends App {
  // mètode quicksort per ordenar de més gran a més petit un list de tuples de (String, Int), només per mostrar els resultats
  def quicksortf[A](xs: Array[(A, Int)]): Array[(A, Int)] =
    if xs.length <= 1 then xs
    else
      val pivot: (A, Int) = xs(xs.length / 2)
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

  // donada una llista de As, retorna una llista de tuples de grups d'As amb el nombre de vegades que apareix a la llista
  def llistaFreqs[A](listTotal: List[A]): List[(A, Int)] = {
    // obtenir la llista de frequències amb les funcions: groupby (agrupa paraules iguals) i map (per cada grup retorna (paraula, longitud de la paraula))
    // i amb toList, converteix el retorn del map, un HashMap[(String, Int)], a una List[(String, Int)]
    listTotal.groupBy(x => x).map(x => (x._1, x._2.length)).toList
  }

  // Donada una llista de paraules i una llista de freqüències de paraules, les mostra amb la sortida de l'exemple
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

    // Segon pas, obtenir la llista de frequències de paraules
    val listCountWords = llistaFreqs(listTotalWords)

    // Tercer pas, mostrar els resultats
    mostrarResults(listTotalWords, listCountWords)

    listCountWords
  }

  def nonstopfreq(textFitxer: String, stopWords: List[String]): List[(String, Int)] = {
    // Primer pas, normalitzar a un String format de paraules en minúscules separades per espais
    val listTotalWords = normalitzar(textFitxer)

    // Segon pas, eliminem els stop-words de la llista de totes les paraules amb la funció filterNot, funciona com filter amb not
    val listNonStopWords = listTotalWords.filterNot(stopWords `contains` _)

    // Tercer pas, obtenir la llista de frequències
    val listCountWords = llistaFreqs(listNonStopWords)

    // Quart pas, mostrar resultats
    mostrarResults(listNonStopWords, listCountWords)

    listCountWords
  }

  def paraulafreqfreq(textFitxer: String): Unit = {
    // Primer pas, normalitzar a un String format de paraules en minúscules separades per espais
    val listTotalWords = normalitzar(textFitxer)
    // I obtenir la llista de frequències
    val listCountWords = llistaFreqs(listTotalWords)

    // Segon pas, ens quedem només amb el nombre d'aparicions de cada paraula (frequencies)
    val listFreqsWords = listCountWords `map` (x => x._2)

    // Tercer pas, compta quantes vegades apareix una freqüència, com comptar la freqüència de les freqüències
    val listFreqsFreqsWords = `quicksortf` (llistaFreqs(listFreqsWords).toArray).toList

    // Mostra els resultats
    println("Les 10 frequencies mes frequents:")
    for (ff <- listFreqsFreqsWords.take(10)) println(s"${ff._2} paraules apareixen ${ff._1} vegades")
    println("\nLes 5 frequencies menys frequents:")
    for (ff <- listFreqsFreqsWords.reverse.take(5)) println(s"${ff._2} paraules apareixen ${ff._1} vegades")
  }

  def ngrames(textFitxer: String, n: Int): List[(String, Int)] = {
    // Primer pas, normalitzar a un String format de paraules en minúscules separades per espais
    val listTotalWords = normalitzar(textFitxer)

    // Segon pas, donada la llista de paraules, amb sliding, s'agafen les paraules de tres en tres (paràmetre n de sliding) i es converteix en un string separats per " "
    val listNGrames = listTotalWords.sliding(3).toList `map` (x => x.mkString(" "))

    // Tercer pas, obtenir la llista de freqüències de ngrames
    val listCountNGrames = llistaFreqs(listNGrames)

    // Quart pas, mostrar els resultats
    mostrarResults(listNGrames, listCountNGrames)

    listCountNGrames
  }

  // Fixem els dos fitxes per fer proves
  val documentsDir = "assets/simil/documents"
  val fitxer1 = documentsDir + "/pg11.txt"
  val fitxer2 = documentsDir + "/pg12.txt"

  // Llegim el contingut dels dos fitxers com a text pla
  val textFitxer1 = ProcessListStrings.llegirFitxer(fitxer1)
  val textFitxer2 = ProcessListStrings.llegirFitxer(fitxer2)

  // Freqüències de paraules
  println("Freqüències de paraules")
  val freqsWords = Simil.freq(textFitxer1)
  println()

  // Sense stop-words
  println("Sense stop-words")
  val listStopWords = ProcessListStrings.llegirFitxer("assets/simil/stopwords/english-stop.txt").replaceAll("'", "\n").split("\n")
  val freqsWordsNonStop = Simil.nonstopfreq(textFitxer1, listStopWords.toList)
  println()

  // Distribució de paraules
  println("Distribució de paraules")
  Simil.paraulafreqfreq(textFitxer1)
  println()

  // ngrames
  println("Ngrames")
  val freqsNGrames = Simil.ngrames(textFitxer1, 3)
  println()

  // Vector space model
  println("Vector space model")
  println()
}
