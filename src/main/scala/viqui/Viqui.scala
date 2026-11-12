package edu.udg.pda
package viqui

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


// classe MapReduce i definició d'una funció MR que abstragui l'ús del MapReduce

// L'Actor MapReduce és polimòrfic amb els tipus de les claus valor de l'entrada [K1,V1], la clau i valor intermitjos [k2,v2] i la clau i valor finals [K2,V3].
// - input és el paràmetre d'entrada (compte perquè depedent de la mida pot ser un problema)
// - mapping és la funció dels mappers
// - reducing és la funció dels reducers
// - NOUS PARÀMETRES: maxMapping i maxReducing, corresponen al número màxim de mappers i reducers que es poden crear
class MapReduce[K1, V1, K2, V2, V3](
                                     input: List[(K1, List[V1])],
                                     mapping: (K1, List[V1]) => List[(K2, V2)],
                                     reducing: (K2, List[V2]) => (K2, V3),
                                     maxMapping: Int, maxReducing: Int) extends Actor {

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
      //println("Hem rebut lencarrec")
      client = sender() // Ens apuntem qui ens ha fet l'encàrrec per enviar-li el missatge més tard.

      // CANVI: farem un mapper per parella (K1,List[V1]) de l'input i el màxim nombre de mappers
      nmappers = Math.min(input.length, maxMapping)

      //println(s"Going to create $nmappers MAPPERS!!")

      // Al crear actors a dins d'altres actors, enlloc de crear el ActorSystem utilitzarem context i així es va organitzant la jerarquia d'actors.
      // D'altra banda, quan els actors que creem tenen un constructor amb paràmetre,
      // no passem el "tipus" de l'actor i prou a Props sino que creem l'actor amb els paràmetres que necessita. En aquest cas, l'Actor mapping és paramètric en tipus
      // i necessita com a paràmetre una funció de mapping.

      val mappers: Seq[ActorRef] = for (i <- 0 until nmappers) yield {
        context.actorOf(Props(new Mapper(mapping)), "mapper" + i)
      }
      // No és necessari passar els tipus K1,V1, ... ja que els infereix SCALA pel paràmetre mapping. D'altra banda compte perquè  "0 until n" és el mateix que "0 to n-1".
      // val mappers = for (i <- 0 to nmappers-1) yield {
      //      context.actorOf(Props(new Mapper[K1,V1,K2,V2](mapping)), "mapper"+i)
      // }

      // Posar les anotacions de tipus aquí no és necessari però ajuda a llegir que a cada mapper li enviem una clau de tipus K1 i una llista de valors de tipus V1

      // Aquesta versió és quadràtica perquè l'accés a input(i) te cost i mentre que la versió de sota amb el zipWithIndex senzillament recorre l'input un cop linealment.
      // AQUEST CANVI ÉS CLAU EN EL RENDIMENT
      //for(i<- 0 until nmappers) mappers(i) ! toMapper(input(i)._1:K1, input(i)._2: List[V1])
      for (((p1, p2), i) <- input.zipWithIndex) mappers(i % nmappers) ! toMapper(p1: K1, p2: List[V1])

      // Per tant, alternativament...
      // for(i<- 0 until nmappers) mappers(i) ! toMapper(input(i)._1, input(i)._2)

      // Necessitem controlar quant s'han acabat tots els mappers per poder llençar els reducers després...
      mappersPendents = input.length

      //println("All sent to Mappers, now start listening...")

    // Anem rebent les respostes dels mappers i les agrupem al diccionari per clau.
    // Tornem a necessitar anotar els tipus del paràmetre que reb fromMapper tal com ho hem fet o be en el codi comentat al generar les tuples.

    case fromMapper(list_clau_valor: List[(K2, V2)]) =>
      for ((clau, valor) <- list_clau_valor)
        dict += (clau -> (valor :: dict(clau)))

      mappersPendents -= 1

      // Quan ja hem rebut tots els missatges dels mappers:
      if (mappersPendents == 0) {
        // creem els reducers, tants com entrades al diccionari; fixeu-vos de nou que fem servir context i fem el new pel constructor del Reducer amb paràmetres
        // CANVI: el nombre de reducers serà entre la mida del diccionari i el número màxim de reducers
        nreducers = Math.min(dict.size, maxReducing)
        reducersPendents = dict.size // actualitzem els reducers pendents

        //println(s"Going to create $nreducers REDUCERS for reduce ${dict.size} pages!!")

        val reducers = for (i <- 0 until nreducers) yield
          context.actorOf(Props(new Reducer(reducing)), "reducer" + i)
        // No cal anotar els tipus ja que els infereix de la funció reducing
        //context.actorOf(Props(new Reducer[K2,V2,V3](reducing)), "reducer"+i)

        // Ara enviem a cada reducer una clau de tipus V2 i una llista de valors de tipus K2.
        // Les anotacions de tipus no caldrien perquè ja sabem de quin tipus és dict, però ens ajuden a documentar.
        //for ((i, (key: K2, lvalue: List[V2])) <- (0 until nreducers) zip dict)
        //  reducers(i) ! toReducer(key, lvalue)
        for (((key, lvalue), i) <- dict.zipWithIndex) reducers(i % nreducers) ! toReducer(key: K2, lvalue: List[V2])
        //println("All sent to Reducers")
      }

    // A mesura que anem rebent respostes del reducer, tuples (K2, V3), les anem afegint al Map del resultatfinal i descomptem reducers pendents. Tornem a necessitar anotar el tipus.
    case fromReducer(entradaDictionari: (K2, V3)) =>
      resultatFinal += entradaDictionari
      reducersPendents -= 1

      // En arribar a 0 enviem a qui ens ha encarregat el MapReduce el resultat. De fet l'està esperant pq haurà fet un ask.
      if (reducersPendents == 0) {
        client ! resultatFinal
        //println("All Done from Reducers!")
        // Ara podem alliberar els recursos dels actors, el propi MapReduce, tots els mappers i els reducers.
        // Fixem-nos que no te sentit que tornem a engegar "aquest mateix" MapReduce.
        context.stop(self)
      }
  }
}

def MR[K1, V1, K2, V2, V3](input: List[(K1, List[V1])],
                           mapping: (K1, List[V1]) => List[(K2, V2)],
                           reducing: (K2, List[V2]) => (K2, V3),
                           maxMapping: Int, maxReducing: Int): Map[K2, V3] = {
  val nmappers = 1
  val nreducers = 1

  implicit val timeout: Timeout = Timeout(100.seconds) // L'implicit permet fixar el timeout per a la pregunta
  val sistema: ActorSystem = ActorSystem("sistema") // ActorSystem per a crear els actors

  try {
    // Creem l'actor principal (masterActorInput) encarregat de coordinar el cicle MapReduce.
    // Li passem per paràmetre l'input amb les dades d'entrada i les funcions mapping i reducing, a més dels nous paràmetres màxims
    val actorInput = sistema.actorOf(
      Props(classOf[MapReduce[K1, V1, K2, V2, V3]], input, mapping, reducing, maxMapping, maxReducing),
      name = "masterActorInput"
    )
    // Fent servir el patró Ask (?) per llançar un missatge que ens retornarà una resposta (les dades de retorn en un objecte de tipus Future)
    val futurResultat = actorInput ? MapReduceCompute() // demanem les dades a l'actor que fa de master
    val resultat: Map[K2, V3] = Await.result(futurResultat, Duration.Inf).asInstanceOf[Map[K2, V3]]

    resultat // Retorna el diccionari del MapReduce calculat
  } finally {
    Thread.sleep(10) // Pausa de seguretat de 10 ms abans de tancar tot el sistema d'actors

    Await.result(sistema.terminate(), 5.seconds) // Tanquem el sistema d'actors
  }
}

// Altres objectes amb funcions útils

object Utilitats {
  // el següent bloc de codi s'avalua només la primera vegada que es crida l'objecte, llavors a la pròxima vegada estarem estalviant temps d'execució
  // bàsicament, llegeix el fitxer amb les stop-words en català, obté una llista de String (de Stop-Words); la normalitza i la retorna
  val STOP_WORDS: List[String] = {
    Simil.normalitzar(ProcessListStrings.llegirFitxer("assets/viquipedia/stopwords/stopwordscatalanet.txt"))
  }

  // donat un bloc de codi de tipus genèric, retorna una tupla amb el retorn del codi i el temps (en segons) que ha trigat en executar-se
  def timeMeasurement[A](block: => A): (A, Double) = {
    val t0 = System.currentTimeMillis() // obté l'instant de temps en milisegons
    val retorn = block // executa el bloc de codi
    val t1 = System.currentTimeMillis() // obté l'instant de temps en milisegons

    (retorn, (t1 - t0).toDouble / 1000)
  }

  // donada una llista qualsevol, retorna una llista amb totes les parelles de valors úniques possibles (tots amb tots sense repetició)
  def crearParelles[A](llista: List[A]): List[(A, A)] = {
    // Utilitzar el foldLeft ens permet definir un acumulador per anar emparellant un a un cada valor de la llista no buida, i guardar després les parelles en la llista de tuples buida
    // Al final, la llista buida de la dreta estarà plena amb les parelles de elements, mentre que la llista de l'esquerra queda buida; ens quedem amb la de la dreta
    llista.foldLeft((llista, List.empty[(A, A)]))((acum, el) => (acum._1.tail, acum._2 ::: acum._1.tail.map(y => (el, y))))._2
  }

  // donada una llista de referències/links en cru/raw (sense processar), retorna la mateixa llista amb els links nets
  def netejarRefs(titol: String, refs: List[String]): List[String] = {
    val auxRefsNets = refs.map(raw =>
      val net = raw.stripPrefix("[[").stripSuffix("]]") // eliminem els caràcters "[[" i "]]"
      val partEsq = net.split("\\|").headOption.getOrElse("").trim // ens quedem amb la part de l'esquerra (el link), descartant el text
      val destiFinal = partEsq.split("#").headOption.getOrElse("").trim // el mateix pels links escrits com "link#secció", només volem els links

      if (destiFinal.length > 0 && destiFinal != titol.trim) destiFinal else "" // eliminem autoenllaços i enllaços interns
    )
    val refsNets = auxRefsNets.filter(_.length > 0).distinct // Eliminem cadenes buides i enllaços duplicats
    refsNets
  }

  // donada una llista "raw", en aquest cas raw vol dir amb stop-words, i retorna la mateixa línia sense stop-words
  def extreureStopwords(llistaRaw: List[String]): List[String] = {
    // Per filtrar les paraules que NO siguin stop-words
    llistaRaw.filterNot(STOP_WORDS `contains` _)
  }

  // donat el contingut d'un document, retorna una llista amb les paraules, que no són stop-words, del contingut
  def normalitzarContingut(contingut: String): List[String] = {
    extreureStopwords(contingut.toLowerCase.replaceAll("[^a-záéíóúàèòïüçñ·\\s]", "").split("\\s+").filter(_.nonEmpty).toList)
  }

  // donats dos Llistes de tuples (String, Double) ens dona el seu coeficient de similitud de cosinus, un valor entre 0 (menys similar) i 1 (més similar)
  def cosinesimV2(listFitxer1: List[(String, Double)], listFitxer2: List[(String, Double)]): Double = {
    // En aquesta versió, anem directament a fer que les dues llistes estiguin alienades.
    // Per tant, ajuntem totes les paraules i pesos de tots dos llibres i eliminem els repetits amb distinct
    val listAllWords = (listFitxer1.map(x => x._1) ++ listFitxer2.map(x => x._1)).distinct
    val mapOfPesFitx1 = listFitxer1.toMap // Convertim les llistes de pesos a Map per poder fer servir la funció getOrElse
    val mapOfPesFitx2 = listFitxer2.toMap

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
}

object MRFunctions {
  // Funcions mapping i reducing  (String, (List[String], List[String]))

  // 1. Mitjana de referències per pàgina
  // donada la ruta d'un document, el parseja (analitza) i retorna una única clau qualsevol (igual per tots els documents) i una tupla amb el nombre de links nets i un document
  def mappingMitjanaRefs(document: String, llistaBuida: List[String]): List[(String, (Int, Int))] = {
    val res = ViquipediaParse.parseViquipediaFile(document) // parseja el document
    res match
      case ViquipediaParse.ResultViquipediaParsing(titol, _, refs) =>
        val refsNets = Utilitats.netejarRefs(titol, refs) // neteja els links

        List(("mitjana_total_refs", (refsNets.length, 1)))
      case _ => List(("", (0, 0)))
  }
  // suma tots els links de tots els documents i el total de documents, després calcula i retorna la mitjana global
  def reducingMitjanaRefs(titol: String, totalRefsAndTotalPagines: List[(Int, Int)]): (String, Double) = {
    val totalRefs = totalRefsAndTotalPagines.map(_._1).sum
    val totalPagines = totalRefsAndTotalPagines.map(_._2).sum

    val mitjana = if (totalPagines > 0) then totalRefs.toDouble / totalPagines else 0.0

    (titol, mitjana)
  }

  // 2. Cerca documents per paraules clau dins d'una query (consulta)
  // donat un document i una query (llista de paraules clau), filtra els documents que contenen totes les paraules de la query (al títol, als links o al cos)
  def mappingCercaDocuments(document: String, query: List[String]): List[(String, (List[String], List[String]))] = {
    val res = ViquipediaParse.parseViquipediaFile(document)
    res match
      case ViquipediaParse.ResultViquipediaParsing(titol, cos, refs) =>
        val refsNets = Utilitats.netejarRefs(titol, refs) // neteja els links
        val wordsCosNet = Utilitats.normalitzarContingut(cos) // neteja el cos

        // comprova si totes les paraules de la query estan al títol o a la llista de links nets o a la llista de paraules del cos
        if query.forall(word => (titol `contains` word) || (refsNets `contains` word) || (wordsCosNet `contains` word)) then
          List((titol, (refsNets, wordsCosNet)))
        else
          Nil
      case _ => Nil
  }
  // retorna el títol del document i una tupla amb la llista dels links nets i la llista de paraules del cos
  def reducingCercaDocuments(titol: String, refsAndCos: List[(List[String], List[String])]): (String, (List[String], List[String])) = {
    (titol, refsAndCos.head)
  }

  // 3. Calcul del PageRank següent en cada iteració
  // donat un document amb títol i una tupla amb una llista de links sortints (kappa), el PageRank anterior, el nombre de documents en total i el nombre de document que no tenen cap link de sortida
  def mappingPRSeguent(titol: String, dades: List[(List[String], Double, Int, Double)]): List[(String, (Double, Int, Double))] = {
    val (links, prAnt, nDocs, sumaDocsSenseLinks) = dades.head

    val cua = if links.nonEmpty then
      val pes = prAnt / links.length.toDouble // el PageRank anterior / kappa(document actual amb titol == titol)
      links.map(x => (x, (pes, nDocs, sumaDocsSenseLinks)))
    else
      Nil

    // retorna només una llista amb els documents que tenen almenys un enllaç de sortida
    List((titol, (0.0, nDocs, sumaDocsSenseLinks))) ::: cua
  }
  // implementació de la fórmula completa del PageRank descrita a l'enunciat de la pràctica
  def reducingPRSeguent(titol: String, dades: List[(Double, Int, Double)]): (String, Double) = {
    val gamma = 0.85 // probabilitat de seguir navegant fent clic a un enllaç
    // recuperem les constants globals (N i la suma dels documents en total i sense enllaços)
    val (_, nDocs, sumaDocsSenseLinks) = dades.head
    val N = nDocs.toDouble

    val sumaDocsAmbLinks = dades.map(_._1).sum // calculem el primer sumatori amb els documents que enllacen amb aquest document
    val saltAleatoriExt = (1.0 - gamma) / N // calculem el salt aleatori
    val sumatori1 = (1.0 / N) * sumaDocsSenseLinks // calculem el segon sumatori amb els documents sense links sortints

    // i acabem de muntar la fórmula de teòria amb el factor de frenada
    val nouPR = saltAleatoriExt + gamma * (sumaDocsAmbLinks + sumatori1)
    (titol, nouPR)
  }

  // 4. Càlcul  del tf_idf per poder comparar la similitud de dos documents
  // donat un document amb títol i una llista de paraules i enllaços, compta la freqüència de cada paraula dins del seu cos
  def mappingSimilitudDocuments(titol: String, refsAndCos: List[(List[String], List[String])]): List[(String, (String, Int))] = {
    val cos = refsAndCos.head._2
    val freqs = Simil.llistaFreqs(cos)

    // després, inverteix la clau per, donada una paraula aconseguir el títol del document on apareix i amb quina freqüència
    freqs.map((paraula, freq) => (paraula, (titol, freq)))
  }
  // agrupa per cada paraula tots els documents on apareix i calcula en quants documents diferents surt (df)
  def reducingSimilitudDocuments(paraula: String, dades: List[(String, Int)]): (String, (Int, List[(String, Int)])) = {
    val numDocs = dades.length

    (paraula, (numDocs, dades)) // retorna per cada paraula, en quants documents apareix i la llista dels documents i vegades que apareix en aquest document
  }

  // 5. Càlcul de la similitud cosinus per parelles de documents
  // donada una parella composta pel document A i pel document B, les passa directament al reducer
  def mappingCalculSimilitudCosinus(fitxer: (String, String), parella: List[(String, List[(String, Double)], String, List[(String, Double)])]): List[((String, String), (List[(String, Double)], List[(String, Double)]))] = {
    val (_, parellaA, _, parellaB) = parella.head

    List((fitxer, (parellaA, parellaB)))
  }
  // fent ús de la funció "cosinesimV2" (igual que a la primera part, modificada) i calcula la similitud del cosinus entre els dos documents
  def reducingCalculSimilitudCosinus(parella: (String, String), llistaParaulesPesos: List[(List[(String, Double)], List[(String, Double)])]): ((String, String), Double) = {
    val (parellaA, parellaB) = llistaParaulesPesos.head

    val sim = Utilitats.cosinesimV2(parellaA, parellaB)

    (parella, sim)
  }
}

object Viqui extends App {
  // Funcions auxiliars

  // donada la llista de documents, retorna el PageRank inicial per cada títol de document
  def calculPRInicial(docs: List[(String, (List[String], List[String]))]): List[(String, Double)] = {
    val initPR = 1.0 / docs.length
    for (i <- docs) yield (i._1, initPR)
  }

  // donada la llista de documents, retorna el PageRank següent per cada títol de document tenint en compte el PageRank anterior
  def calculPRSeguent(docs: List[(String, (List[String], List[String]))], prAnt: List[(String, Double)]): List[(String, Double)] = {
    val N = docs.length.toDouble
    val mapPR = prAnt.toMap
    val gamma = 0.85 // definim el factor de frenada gamma
    val saltAleatoriExt = (1.0 - gamma) / N

    val midaKappa = docs.map(x => (x._1, x._2._1.length)).toMap
    val docsAmbSet = docs.map(x => (x._1, x._2._1.toSet)) // convertim la llista de links a un conjunt de links perquè vagi més ràpid

    // calculem la segona part de la formula per calcular el seguent, amb els documents que no enllacen amb ningu
    val docsSenseLinks = docs.filter(x => midaKappa(x._1) == 0)
    val sumaDocsSenseLinks = docsSenseLinks.map(x => mapPR(x._1)).sum
    val sumatori1 = (1.0 / N) * sumaDocsSenseLinks

    // iterem per calcular la primera part amb cada document i els documents enllacen a ell mateix, i posteriorment, unificar amb la segona part
    for (i <- docs) yield
      // calculem la primera part del sumatori
      val docsAmbLinks = docsAmbSet.filter(_._2 `contains` i._1).map(_._1).filter(x => midaKappa(x) > 0)
      val sumaDocsAmbLinks = docsAmbLinks.map(x => mapPR(x) / midaKappa(x).toDouble).sum

      // ajuntem les dues parts de la formula, afegint el factor de frenada gamma
      val nouPR = saltAleatoriExt + gamma * (sumaDocsAmbLinks + sumatori1)

      (i._1, nouPR)
  }

  // donada la llista de documents, calcula de forma iterativa per cada document el seu PageRank, fent servir l'algorisme explicat a l'enunciat de la pràctica, fins que convergeixi
  def calculPR(docs: List[(String, (List[String], List[String]))]): List[(String, Double)] = {
    def bucleCalculPR(t: Int, PRAct: List[(String, Double)]): List[(String, Double)] = {
      val PRSeg = calculPRSeguent(docs, PRAct) // calculem de cada títol de document, el PageRank següent

      // calculem la diferència absoluta acumulada entre l'iteració actual i la següent per comprovar si ha convergit
      val mapAct = PRAct.toMap
      val mapSeg = PRSeg.toMap
      val diffTotal = docs.map(x => Math.abs(mapSeg(x._1) - mapAct(x._1))).sum

      // comprovem si ha convergit (hem arribat al final de l'algorisme) i si s'ha arribat al màxim nombre d'iteracions
      val maxIter = t >= 5
      val convergit = diffTotal <= 1e-3

      // llavors, l'algorisme acaba quan...
      (maxIter, convergit) match {
        case (true, _) => PRSeg // s'ha assolit el nombre màxim d'iteracions
        case (false, true) => PRSeg // l'algorisme del PageRank ha convergit
        case (false, false) => bucleCalculPR(t + 1, PRSeg) // si no passa res de l'anterior, fem una iteració més de l'algorisme del PageRank
      }
    }

    // obtenim el PageRank inicial i comencem l'algorisme
    val PRIni = calculPRInicial(docs)
    bucleCalculPR(1, PRIni)
  }

  // Codi principal

  // variables inicials
  val configuracio = 20 // List(1, 4, 10, 20)
  val documentsDir = "assets/viquipedia/documents"

  println(s"================== Prova amb $configuracio actors ==================")

  // obtenim la llista de documents, només les rutes absolutes, en una llista
  val llistaDocuments = ProcessListStrings.getListOfFiles(documentsDir).map(f => (f.getAbsolutePath, List()))

  // calcul del nombre mitjà de referències que tenen totes les pàgines
  val (calculMitjanaRefs, temps) = Utilitats.timeMeasurement(MR(llistaDocuments, MRFunctions.mappingMitjanaRefs, MRFunctions.reducingMitjanaRefs, configuracio, configuracio))
  println(s"================== MITJANA DE REFERÈNCIES EN TOTS ELS DOCUMENTS ==================")
  println(s"La mitjana de referències per pàgina és: ${calculMitjanaRefs.head._2}")
  println(s"Ha trigat $temps segons")

  println()

  // donada una query, fer una recomanació fent ús de l’algoritme Page-Rank
  println("\n================ CERCA DOCUMENTS ================")
  val query = "Segona Guerra Mundial"

  val normalQuery = Simil.normalitzar(query)
  val normalFilteredQuery = Utilitats.extreureStopwords(normalQuery)

  val (docsTrobats, temps2) = Utilitats.timeMeasurement(MR(llistaDocuments.map(f => (f._1, normalFilteredQuery)), MRFunctions.mappingCercaDocuments, MRFunctions.reducingCercaDocuments, configuracio, configuracio))
  println(s"Ha trigat $temps2 segons")

  val (docsTrobatsOrd, temps3) = Utilitats.timeMeasurement(calculPR(docsTrobats.toList).sortBy(-_._2))
  println(s"Ha trigat $temps3 segons")
  println("\n================ TOP 100 PÀGINES RECOMANADES ================")
  val rankingTop100 = docsTrobatsOrd.take(100)
  for (i <- rankingTop100) yield println(s"Pàgina ${i._1}. PageRank ${i._2}")

  println()

  // detectar les pàgines que s’assemblen més, però no es referencien mútuament
  println("\n================ PÀGINES SEMBLANTS ================")
  val titolsTop100 = rankingTop100.map(_._1).toSet
  val top100Docs = docsTrobats.filter(x => titolsTop100 `contains` x._1).map(x => (x._1, List(x._2))).toList

  val (top100DocsSim, temps4) = Utilitats.timeMeasurement(MR(top100Docs, MRFunctions.mappingSimilitudDocuments, MRFunctions.reducingSimilitudDocuments, configuracio, configuracio))
  println(s"Ha trigat $temps4 segons")

  val idfs = for (i <- top100DocsSim) yield (i._1, Math.log(top100Docs.length / i._2._1))
  val mapIdfs = idfs

  var tfIdFinal = for (i <- top100DocsSim; idf = mapIdfs(i._1); j <- i._2._2) yield
    val pes = j._2 * idf
    (j._1, i._1, pes)

  val docsSim = tfIdFinal.groupBy(_._1).map(x => (x._1, x._2.map(y => (y._2, y._3)))).toList

  val mapDocsSim = docsSim.toMap
  val mapNomsDocsRes = mapDocsSim.keys.toList
  val parellesNoms = Utilitats.crearParelles(mapNomsDocsRes)

  val inputMR = parellesNoms.map(x =>
    val titol1 = x._1
    val titol2 = x._2

    val parellaA = mapDocsSim(titol1).toList
    val parellaB = mapDocsSim(titol2).toList

    ((titol1, titol2), List((titol1, parellaA, titol2, parellaB)))
  )

  val (documentsMesSimilars, temps5) = Utilitats.timeMeasurement(MR(inputMR, MRFunctions.mappingCalculSimilitudCosinus, MRFunctions.reducingCalculSimilitudCosinus, configuracio, configuracio))
  val rankingDocumentsMesSimilars = documentsMesSimilars.filter(_._2 > 0.5)
  println(s"Ha trigat $temps5 segons")
  println("\n================ PARELLES DE DOCUMENTS MÉS SIMILARS ================")
  for (i <- rankingDocumentsMesSimilars) yield println(s"Documents ${i._1._1} i ${i._1._2}. Similitud ${i._2}")
}
