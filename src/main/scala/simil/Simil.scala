package edu.udg.pda
package simil

import utils.ProcessListStrings

object Simil extends App {
  // Funcions auxiliars

  // donat un array de (A, Int), retorna el mateix array ordenat de més gran a més petit
  def quicksortf[A](xs: Array[(A, Int)]): Array[(A, Int)] =
    if xs.length <= 1 then xs
    else
      val pivot: (A, Int) = xs(xs.length / 2)
      Array.concat(
        quicksortf(xs filter (x => pivot._2 < x._2)),
        xs filter (x => x._2 == pivot._2),
        quicksortf(xs filter (x => pivot._2 > x._2))
      )

  // donat un llibre en text pla (string) retorna el mateix text en minúscula i sense els caràcters que no fan paraules
  def normalitzar(textFitxer: String): List[String] = {
    val textFitxerMinus = textFitxer.toLowerCase() // passem totes les lletres a mínuscules

    val textFitxerLletres = textFitxerMinus.replaceAll("[^a-z\\s]", " ") // eliminem tot els caràcters que no fan paraules

    val textFitxerNorm = textFitxerLletres.replaceAll("\\s+", " ") // eliminem els espais en blanc sobrants deixant només un espai en blanc

    textFitxerNorm.trim().split(" ").toList // dividim el string per espais i generem una llista
  }

  // donada una llista de As, retorna una llista de tuples de grups d'As amb el nombre de vegades que apareix a la llista
  def llistaFreqs[A](listTotal: List[A]): List[(A, Int)] = {
    // obtenir la llista de frequencies amb les funcions: groupby i map. I amb toList converteix el HashMap[(A, Int)] a una List[(A, Int)]
    // Amb la funcio "groupby" es genera un HashMap de tuples (A, List(A)), on si A fos un String, es cada paraula acompanyat per una llista amb la mateixa paraula repetida n vegades. Ex: per "hola", la segona posicio sera una tupla ("hola", List("hola","hola",...))
    // Amb la funcio "map", partint del HashMap[A, List(A)], transforma cada tupla en (A, longitud de la llista de As), o sigui en HashMap[(A, Int)]. Ex: per ("hola", List("hola","hola",...)) sera ("hola", 2)
    listTotal.groupBy(x => x).map(x => (x._1, x._2.length)).toList
  }

  // donada una llista de paraules i una llista de freqüències absolutes de paraules, mostra les deu més frequents amb la sortida de l'enunciat
  def mostrarResults(listTotalWords: List[String], listCountWords: List[(String, Int)]): Unit = {
    val listOrd = quicksortf(listCountWords.toArray).take(10) // ordena la llista listCountWords i obtenim els deu primers
    // mostrar titol i capsalera
    println(s"Num de paraules: ${listTotalWords.length}\tDiferents: ${listCountWords.length}")
    println("Paraules\tocurrencies\tfrequencia")
    println("---------------------------------------")
    // mostrar resultats
    for f <- listOrd do
      // calcula el percentatge i mostra la fila X
      val percentage = (f._2.toDouble / listTotalWords.length) * 100
      println(s"${f._1}\t${f._2}\t${String.format("%.2f", percentage).replace(".", ",")}")
  }

  // donada una llista de tuples (String, Int), on Int són les n vegades que apareix una paraula String; retorna una llista de String i pesos (Double)
  def freqToPes(listWords: List[(String, Int)]): List[(String, Double)] = {
    val maxList = listWords.map(x => x._2).max // troba el maxim, com la frequencia absoluta de la paraula que apareix mes vegades

    listWords.map(x => (x._1, x._2.toDouble / maxList)) // Calcula el pes de cada paraula, dividint la frequencia absoluta amb el maxim
  }

  // Funcions principals

  // donat un llibre en text pla (string), retorna una llista de tuples de paraules i el nombre d'aparicions
  def freq(textFitxer: String, mostrar: Boolean = false): List[(String, Int)] = {
    // Primer pas, normalitzar a un String format de paraules en minúscules separades per espais
    val listTotalWords = normalitzar(textFitxer)

    // Segon pas, obtenir la llista de frequències de paraules
    val listCountWords = llistaFreqs(listTotalWords)

    // Tercer pas, mostrar els resultats
    if mostrar then mostrarResults(listTotalWords, listCountWords)

    listCountWords
  }

  // exactament el mateix que freq, però eliminant les stop-words
  def nonstopfreq(textFitxer: String, stopWords: List[String], mostrar: Boolean = false): List[(String, Int)] = {
    // Primer pas, normalitzar a un String format de paraules en minúscules separades per espais
    val listTotalWords = normalitzar(textFitxer)

    // Segon pas, eliminem els stop-words de la llista de totes les paraules amb la funcio filterNot: funciona com filter, però amb not
    val listNonStopWords = listTotalWords.filterNot(stopWords `contains` _)

    // Tercer pas, obtenir la llista de frequències
    val listCountWords = llistaFreqs(listNonStopWords)

    // Quart pas, mostrar resultats
    if mostrar then mostrarResults(listNonStopWords, listCountWords)

    listCountWords
  }

  // partint de freq, calcula les freqüències absolutes de les freqüències absolutes de cada paraula
  def paraulafreqfreq(textFitxer: String): Unit = {
    // Primer pas, normalitzar a una List[String] i obtenir la llista de frequències
    val listCountWords = freq(textFitxer)

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

  // donada una n i un String, dels ngrames de l’String rebut, troba les freqüències
  def ngrames(textFitxer: String, n: Int, mostrar: Boolean = false): List[(String, Int)] = {
    // Primer pas, normalitzar a un String format de paraules en minúscules separades per espais
    val listTotalWords = normalitzar(textFitxer)

    // Segon pas, donada la llista de paraules, amb sliding, s'agafen les paraules de tres en tres (paràmetre n de sliding) i es converteix en un string separats per " "
    val listNGrames = listTotalWords.sliding(n).toList `map` (x => x.mkString(" "))

    // Tercer pas, obtenir la llista de freqüències de ngrames
    val listCountNGrames = llistaFreqs(listNGrames)

    // Quart pas, mostrar els resultats
    if mostrar then mostrarResults(listNGrames, listCountNGrames)

    listCountNGrames
  }

  // donats dos String ens dona el seu coeficient de similitud de cosinus, un valor entre 0 (menys similar) i 1 (més similar)
  def cosinesim(textFitxer1: String, textFitxer2: String, stopWords: List[String]): Double = {
    // Primer pas, per tots dos llibres, normalitzar, obtenir la llista de frequencies i convertir les frequencies absolutes en pesos
    val listOfPesFitx1 = freqToPes(nonstopfreq(textFitxer1, stopWords))
    val listOfPesFitx2 = freqToPes(nonstopfreq(textFitxer2, stopWords))

    // Segon pas, les dues llistes han d'estar alienades. Per tant, ajuntem totes les paraules i pesos de tots dos llibres i eliminem els repetits amb distinct
    val listAllWords = (listOfPesFitx1.map(x => x._1) ++ listOfPesFitx2.map(x => x._1)).distinct
    val mapOfPesFitx1 = listOfPesFitx1.toMap // Convertim les llistes de pesos a Map per poder fer servir la funció getOrElse
    val mapOfPesFitx2 = listOfPesFitx2.toMap

    // Amb la llista de totes les paraules i pesos, generem els vectors a i b per fer el calcul
    val a = listAllWords.map(x => mapOfPesFitx1.getOrElse(x, 0.0)) // Si la paraula x hi és al llibre 1, el pes sera el pes calculat, altrament sera 0.0
    val b = listAllWords.map(x => mapOfPesFitx2.getOrElse(x, 0.0)) // El mateix pel llibre 2

    // Tercer pas, copiar/adaptar la formula de l'enunciat de la practica
    val numerador = a.zip(b).map((x, y) => x * y).sum // calcul del numerador
    val demEsq = Math.sqrt(a.map(x => x * x).sum) // calcul de la part esquerra del denominador
    val demDre = Math.sqrt(b.map(x => x * x).sum) // calcul de la part dreta del denominador

    // Retornar 0.0, en cas que el denomiandor sigui 0, ja que no es pot dividir per 0; si no es 0, llavors es fa el calcul
    if (demEsq * demDre) == 0 then 0.0 else numerador / (demEsq * demDre)
  }

  // Fi de funcions

  // Fixem els dos fitxes per fer proves
  val documentsDir = "assets/simil/documents"
  val fitxer1 = documentsDir + "/pg11.txt"
  val fitxer2 = documentsDir + "/pg12.txt"

  // Llegim el contingut dels dos fitxers com a text pla
  val textFitxer1 = ProcessListStrings.llegirFitxer(fitxer1)
  val textFitxer2 = ProcessListStrings.llegirFitxer(fitxer2)

  // Freqüències de paraules
  println("Freqüències de paraules")
  val freqsWords = Simil.freq(textFitxer1, mostrar = true)
  println()

  // Sense stop-words
  println("Sense stop-words")
  val listStopWords = ProcessListStrings.llegirFitxer("assets/simil/stopwords/english-stop.txt").replaceAll("'", "\n").split("\n")
  val freqsWordsNonStop = Simil.nonstopfreq(textFitxer1, listStopWords.toList, mostrar = true)
  println()

  // Distribució de paraules
  println("Distribució de paraules")
  Simil.paraulafreqfreq(textFitxer1)
  println()

  // ngrames
  println("Ngrames")
  val freqsNGrames = Simil.ngrames(textFitxer1, 3, mostrar = true)
  println()

  // Vector space model
  println("Vector space model")
  val coefcosinusim = Simil.cosinesim(textFitxer1, textFitxer2, listStopWords.toList)
  println(s"La similitud cosinus entre $fitxer1 i $fitxer2 és $coefcosinusim")
  println()
  
  // Taules amb els resultats de les comparacions finals
  val llibres = ProcessListStrings.getListOfFiles(documentsDir)
  println(s"Num de llibres: ${llibres.length}")
  println("Llibre 1\tllibre 2\tsimilitud")
  println("---------------------------------------")
  for llibre1 <- llibres; llibre2 <- llibres yield
    val sim = cosinesim(ProcessListStrings.llegirFitxer(llibre1.toString), ProcessListStrings.llegirFitxer(llibre2.toString), listStopWords.toList)
    println(s"${llibre1.toString.substring(23)}\t${llibre2.toString.substring(23)}\t${String.format("%.4f", sim).replace(".", ",")}")
}
