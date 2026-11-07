package edu.udg.pda
package viqui

import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.language.postfixOps
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import mapreduce.*
import utils.ProcessListStrings
import utils.ViquipediaParse
import simil.Simil


// Altres classes i objectes

// L'Actor MapReduce és polimòrfic amb els tipus de les claus valor de l'entrada [K1,V1], la clau i valor intermitjos [k2,v2]
// i la clau i valor finals [K2,V3].
// - input és el paràmetre d'entrada (compte perquè depedent de la mida pot ser un problema)
// - mapping és la funció dels mappers
// - reducing és la funció dels reducers
class MapReduce[K1, V1, K2, V2, V3](
                                     input: List[(K1, List[V1])],
                                     mapping: (K1, List[V1]) => List[(K2, V2)],
                                     reducing: (K2, List[V2]) => (K2, V3),
                                     maxMapping: Int,
                                     maxReducing: Int) extends Actor {

  var nmappers = 0 // adaptar per poder tenir menys mappers
  var mappersPendents = 0
  var nreducers = 0 // adaptar per poder tenir menys reducers
  var reducersPendents = 0

  var num_files_mapper = 0
  // dict serà el diccionari amb el resultat intermedi
  var dict: Map[K2, List[V2]] = Map[K2, List[V2]]() withDefaultValue List()
  // resultatFinal recollirà les respostes finals dels reducers
  var resultatFinal: Map[K2, V3] = Map()

  // Ens apuntem qui ens ha demanat la feina
  var client: ActorRef = null


  def receive: Receive = {
    // En rebre el missatge MapReduceCompute engeguem el procés.
    case MapReduceCompute() =>
      println("Hem rebut lencarrec")
      client = sender() // Ens apuntem qui ens ha fet l'encàrrec per enviar-li el missatge més tard.

      // farem un mapper per parella (K1,List[V1]) de l'input

      nmappers = Math.min(input.length, maxMapping)

      println(s"Going to create $nmappers MAPPERS!!")

      // Al crear actors a dins d'altres actors, enlloc de crear el ActorSystem utilitzarem context i així es va
      // organitzant la jerarquia d'actors.
      // D'altra banda, quan els actors que creem tenen un contructor amb paràmetre, no passem el "tipus" de l'actor i prou
      // a Props sino que creem l'actor amb els paràmetres que necessita. En aquest cas, l'Actor mapping és paramètric en tipus
      // i necessita com a paràmetre una funció de mapping.

      val mappers: Seq[ActorRef] = for (i <- 0 until nmappers) yield {
        context.actorOf(Props(new Mapper(mapping)), "mapper" + i)
      }
      // No és necessari passar els tipus K1,V1, ... ja que els infereix SCALA pel paràmetre mapping
      // D'altra banda compte pq  "0 until n" és el mateix que "0 to n-1".
      // val mappers = for (i <- 0 to nmappers-1) yield {
      //      context.actorOf(Props(new Mapper[K1,V1,K2,V2](mapping)), "mapper"+i)
      // }

      // Posar les anotacions de tipus aquí no és necessari però ajuda a llegir que
      // a cada mapper li enviem una clau de tipus K1 i una llista de valors de tipus V1

      // Aquesta versi és quadràtica perquè l'accés a input(i) te cost i mentre
      // que la versió de sota amb el zipWithIndex senzillament recorre l'input un cop
      // linealment. AQUEST CANVI ÉS CLAU EN EL RENDIMENT
      //for(i<- 0 until nmappers) mappers(i) ! toMapper(input(i)._1:K1, input(i)._2: List[V1])

      for (((p1, p2), i) <- input.zipWithIndex) mappers(i % nmappers) ! toMapper(p1: K1, p2: List[V1])

      // Per tant, alternativament...
      // for(i<- 0 until nmappers) mappers(i) ! toMapper(input(i)._1, input(i)._2)

      // Necessitem controlar quant s'han acabat tots els mappers per poder llençar els reducers després...
      mappersPendents = input.length

      println("All sent to Mappers, now start listening...")


    // Anem rebent les respostes dels mappers i les agrupem al diccionari per clau.
    // Tornem a necessitar anotar els tipus del paràmetre que reb fromMapper tal com ho hem fet
    // o be en el codi comentat al generar les tuples.

    case fromMapper(list_clau_valor: List[(K2, V2)]) =>
      for ((clau, valor) <- list_clau_valor)
        dict += (clau -> (valor :: dict(clau)))

      mappersPendents -= 1

      // Quan ja hem rebut tots els missatges dels mappers:
      if (mappersPendents == 0) {
        // creem els reducers, tants com entrades al diccionari; fixeu-vos de nou que fem servir context i fem el new
        // pel constructor del Reducer amb paràmetres
        nreducers = Math.min(dict.size, maxReducing)
        reducersPendents = dict.size // actualitzem els reducers pendents

        println(s"Going to create $nreducers REDUCERS for reduce ${dict.size} pages!!")

        val reducers = for (i <- 0 until nreducers) yield
          context.actorOf(Props(new Reducer(reducing)), "reducer" + i)
        // No cal anotar els tipus ja que els infereix de la funció reducing
        //context.actorOf(Props(new Reducer[K2,V2,V3](reducing)), "reducer"+i)

        // Ara enviem a cada reducer una clau de tipus V2 i una llista de valors de tipus K2. Les anotacions de tipus
        // no caldrien perquè ja sabem de quin tipus és dict, però ens ajuden a documentar.
        //for ((i, (key: K2, lvalue: List[V2])) <- (0 until nreducers) zip dict)
        //  reducers(i) ! toReducer(key, lvalue)
        for (((key, lvalue), i) <- dict.zipWithIndex) reducers(i % nreducers) ! toReducer(key: K2, lvalue: List[V2])
        println("All sent to Reducers")
      }

    // A mesura que anem rebent respostes del reducer, tuples (K2, V3), les anem afegint al Map del resultatfinal i
    // descomptem reducers pendents. Tornem a necessitar anotar el tipus.
    case fromReducer(entradaDictionari: (K2, V3)) =>
      resultatFinal += entradaDictionari
      reducersPendents -= 1


      // En arribar a 0 enviem a qui ens ha encarregat el MapReduce el resultat. De fet l'està esperant pq haurà fet un ask.
      if (reducersPendents == 0) {
        client ! resultatFinal
        println("All Done from Reducers!")
        // Ara podem alliberar els recursos dels actors, el propi MapReduce, tots els mappers i els reducers.
        // Fixem-nos que no te sentit que tornem a engegar "aquest mateix" MapReduce.
        context.stop(self)
      }
  }
}

def MR[K1, V1, K2, V2, V3](input: List[(K1, List[V1])],
                           mapping: (K1, List[V1]) => List[(K2, V2)],
                           reducing: (K2, List[V2]) => (K2, V3),
                           maxMapping: Int,
                           maxReducing: Int): Map[K2, V3] = {
  val nmappers = 1
  val nreducers = 1

  implicit val timeout: Timeout = Timeout(100.seconds) // L'implicit permet fixar el timeout per a la pregunta
  val sistema: ActorSystem = ActorSystem("sistema") // ActorSystem per a crear els actors

  try {
    val actorInput = sistema.actorOf(
      Props(classOf[MapReduce[K1, V1, K2, V2, V3]], input, mapping, reducing, maxMapping, maxReducing),
      name = "masterActorInput"
    )
    val futurResultat = actorInput ? MapReduceCompute() // demanem les dades a l'actor que fa de master
    val resultat: Map[K2, V3] = Await.result(futurResultat, Duration.Inf).asInstanceOf[Map[K2, V3]]

    resultat
  } finally {
    Thread.sleep(10)

    Await.result(sistema.terminate(), 5.seconds)
  }
}


object Utilitats {
  // donat una llista de referències/links, retorna la mateixa llista amb els links nets
  def netejarRefs(titol: String, refs: List[String]): List[String] = {
    val auxRefsNets = refs.map(raw =>
      val net = raw.stripPrefix("[[").stripSuffix("]]")
      val partEsq = net.split("\\|").headOption.getOrElse("").trim
      val destiFinal = partEsq.split("#").headOption.getOrElse("").trim

      if (destiFinal.length > 0 && destiFinal != titol.trim) destiFinal else "" // eliminem autoenllaços i enllaços interns
    )
    val refsNets = auxRefsNets.filter(_.length > 0).distinct
    refsNets
  }

  def extreureStopwords(llistaRaw: List[String], fitxerStopwords: String = "assets/viquipedia/stopwords/stopwordscatalanet.txt"): List[String] = {
    val stopwords = Simil.normalitzar(ProcessListStrings.llegirFitxer(fitxerStopwords))
    val llistaSenseStopwords = llistaRaw.filterNot(stopwords `contains` _)
    llistaSenseStopwords
  }

  // donat el contingut d'un document, retorna una llista amb les paraules, que no són stop-words, del contingut
  def normalitzarContingut(contingut: String): List[String] = {
    extreureStopwords(contingut.toLowerCase.replaceAll("[^a-záéíóúàèòïüçñ·\\s]", "").split("\\s+").filter(_.nonEmpty).toList)
  }
}

object MRFunctions {
  // Funcions mapping i reducing  (String, (List[String], List[String]))
  def mappingMitjanaRefs(document: String, llistaBuida: List[String]): List[(String, (Int, Int))] = {
    val res = ViquipediaParse.parseViquipediaFile(document)
    res match
      case ViquipediaParse.ResultViquipediaParsing(titol, _, refs) =>
        val refsNets = Utilitats.netejarRefs(titol, refs)

        List(("mitjana_total_refs", (refsNets.length, 1)))
      case _ => List(("", (0, 0)))
  }
  def reducingMitjanaRefs(titol: String, totalRefsAndTotalPagines: List[(Int, Int)]): (String, Double) = {
    val totalRefs = totalRefsAndTotalPagines.map(_._1).sum
    val totalPagines = totalRefsAndTotalPagines.map(_._2).sum

    val mitjana = if (totalPagines > 0) then totalRefs.toDouble / totalPagines else 0.0

    (titol, mitjana)
  }

  def mappingCercaDocuments(document: String, query: List[String]): List[(String, (List[String], List[String]))] = {
    val res = ViquipediaParse.parseViquipediaFile(document)
    res match
      case ViquipediaParse.ResultViquipediaParsing(titol, cos, refs) =>
        val refsNets = Utilitats.netejarRefs(titol, refs)
        val wordsCosNet = Utilitats.normalitzarContingut(cos)

        if query.forall(word => (titol `contains` word) || (refsNets `contains` word) || (wordsCosNet `contains` word)) then
          List((titol, (refsNets, wordsCosNet)))
        else
          Nil
      case _ => Nil
  }
  def reducingCercaDocuments(titol: String, refsAndCos: List[(List[String], List[String])]): (String, (List[String], List[String])) = {
    (titol, refsAndCos.head)
  }
}

object Viqui extends App {
  // Funcions auxiliars

  // donat un bloc de codi de tipus genèric, retorna una tupla amb el retorn del codi i el temps (en segons) que ha trigat en executar-se
  def timeMeasurement[A](block: => A): (A, Double) = {
    val t0 = System.currentTimeMillis() // obté l'instant de temps en milisegons
    val retorn = block // executa el bloc de codi
    val t1 = System.currentTimeMillis() // obté l'instant de temps en milisegons

    (retorn, (t1 - t0).toDouble / 1000)
  }

  // donat un document D i una llista de documents, retorna els documents als quals enllaça D
  def kappa(d: String, docs: List[(String, (List[String], List[String]))]): List[String] = {
    docs.filter(_._1 == d).map((_, x) => x._1).head
  }

  // donat un document D i una llista de documents, retorna els documents que enllacen amb D
  def delta(d: String, docs: List[(String, (List[String], List[String]))]): List[String] = {
    docs.filter(_._2._1 `contains` d).map(_._1)
  }

  def calculPRInicial(docs: List[(String, (List[String], List[String]))]): List[(String, Double)] = {
    val initPR = 1.0 / docs.length
    for (i <- docs) yield (i._1, initPR)
  }

  def calculPRSeguent(docs: List[(String, (List[String], List[String]))], prAnt: List[(String, Double)]): List[(String, Double)] = {
    val N = docs.length.toDouble
    val mapPR = prAnt.toMap

    // definim el factor de frenada gamma
    val gamma = 0.85

    val saltAleatoriExt = (1.0 - gamma) / N

    // calculem la segona part de la formula per calcular el seguent, amb els documents que no enllacen amb ningu
    val docsSenseLinks = docs.filter(x => kappa(x._1, docs).isEmpty)
    val sumaDocsSenseLinks = docsSenseLinks.map(x => mapPR(x._1)).sum
    val sumatori1 = (1.0 / N) * sumaDocsSenseLinks

    // iterem per calcular la primera part amb cada document i els documents enllacen a ell, i unificar amb la segona part
    for (i <- docs) yield
      // calculem la primera part del sumatori
      val docsAmbLinks = delta(i._1, docs).filter(x => kappa(x, docs).nonEmpty)
      val sumaDocsAmbLinks = docsAmbLinks.map(x => mapPR(x) / kappa(x, docs).length.toDouble).sum

      // ajuntem les dues parts de la formula, afegint el factor de frenada gamma
      val nouPR = saltAleatoriExt + gamma * (sumaDocsAmbLinks + sumatori1)

      (i._1, nouPR)
  }

  def calculPR(docs: List[(String, (List[String], List[String]))]): List[(String, Double)] = {
    def bucleCalculPR(t: Int, PRAct: List[(String, Double)]): List[(String, Double)] = {
      val PRSeg = calculPRSeguent(docs, PRAct)

      val mapAct = PRAct.toMap
      val mapSeg = PRSeg.toMap
      val diffTotal = docs.map(x => Math.abs(mapSeg(x._1) - mapAct(x._1))).sum

      val maxIter = t >= 5
      val convergit = diffTotal <= 1e-3

      (maxIter, convergit) match {
        case (true, _) => PRSeg
        case (false, true) => PRSeg
        case (false, false) => bucleCalculPR(t + 1, PRSeg)
      }
    }

    val PRIni = calculPRInicial(docs)
    bucleCalculPR(1, PRIni)
  }

  def generarLlistaFitxersRefs(fitxer: String): (String, (List[String], List[String])) = {
    val res = ViquipediaParse.parseViquipediaFile(fitxer)
    res match
      case ViquipediaParse.ResultViquipediaParsing(titol, cos, refs) =>
        // Netegem els links de manera eficient un sol cop
        val refsNets = Utilitats.netejarRefs(titol, refs)

        // Netejem el cos
        val cosSenseRefs = cos.replace("[[", " ").replace("]]", " ").replace("|", " ").trim
        val cosNet = cosSenseRefs.toLowerCase.replaceAll("[^a-záéíóúàèòïüçñ·\\s]", "").split("\\s+").filter(_.nonEmpty).toList

        (titol, (refsNets, cosNet))
      case _ => ("", (List(), List()))
  }

  // variables inicials
  val configuracio = 20 // List(1, 4, 10, 20)
  val documentsDir = "assets/viquipedia/documents"

  val llistaDocuments = ProcessListStrings.getListOfFiles(documentsDir).map(f => (f.getAbsolutePath, List()))

  // calcul del nombre mitjà de referències que tenen totes les pàgines
  val (resultat, temps) = timeMeasurement(MR(llistaDocuments, MRFunctions.mappingMitjanaRefs, MRFunctions.reducingMitjanaRefs, configuracio, configuracio))
  println(s"================== Prova amb $configuracio actors ==================")
  println(s"La mitjana de referències per pàgina és: $resultat")
  println(s"Ha trigat $temps segons")

  // donada una query, fer una recomanació fent ús de l’algoritme Page-Rank
  val query = "Segona Guerra Mundial"

  val normalQuery = Simil.normalitzar(query)
  val normalFilteredQuery = Utilitats.extreureStopwords(normalQuery)

  val (resultat2, temps2) = timeMeasurement(MR(llistaDocuments.map(f => (f._1, normalFilteredQuery)), MRFunctions.mappingCercaDocuments, MRFunctions.reducingCercaDocuments, configuracio, configuracio))

  val (rankingFinal, temps3) = timeMeasurement(calculPR(resultat2.toList).sortBy(-_._2))

  println(s"Temps: $temps3 segons")
  println("\n================ TOP 10 PÀGINES RECOMANADES ================")
  rankingFinal.take(100).foreach { case (titol, score) =>
    println(s"- Pàgina: $titol | Score PR: $score")
  }

  // detectar les pàgines que s’assemblen més, però no es referencien mútuament
}
/*
* • Punt 2 (Filtre textual i PageRank per Query):
	• Has de demanar la query a l'usuari per consola (readLine()).
	• Has de netejar la query de stop-words (com de, la, els).
	• Has d'utilitzar un segon MapReduce (complint el Punt 8) sobre la teva llista carregada en memòria per filtrar i quedar-te només amb les pàgines que continguin tots els mots de la query.
	• Has de programar l'algorisme PageRank iteratiu de punt fix (fins a 5 iteracions o convergència) per ordenar aquest subgraf.
• Punt 3 (Càlcul de similitud TF-IDF i anomalies estructural):
	• Agafant com a molt les 100 pàgines amb millor PageRank del pas anterior, has de calcular els vectors TF-IDF de cadascuna (filtrant enllaços i stop-words dins del tag <text>).
	• Has d'aplicar la Similitud Cosinus entre elles per parelles.
	• Has de llistar aquelles que tinguin una similitud > 0.5 però que no es referencien mútuament (anomalies).
• Punt 8 (Ús generalitzat de MapReduce): S'anirà completant a mesura que utilitzis el framework d'actors per al filtratge textual i per al càlcul de les freqüències de paraules del TF-IDF.
* */
