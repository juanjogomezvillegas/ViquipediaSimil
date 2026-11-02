package edu.udg.pda
package viqui

import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.language.postfixOps
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import mapreduce.{MapReduce, *}
import utils.ProcessListStrings
import utils.ViquipediaParse

import akka.pattern.ask
import akka.util.Timeout

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

object MRFunctions {
  // Funcions mapping i reducing
  def mappingMitjanaRefs(fitxer: String, rutes: List[String]): List[(String, (Int, Int))] = {
    val res = ViquipediaParse.parseViquipediaFile(rutes.head)
    res match
      case ViquipediaParse.ResultViquipediaParsing(_, _, refs) => List((fitxer, (refs.distinct.length, 1)))
      case _ => List()
  }

  def reducingGetMitjanaRefs(fitxer: String, numTotalRefs: List[(Int, Int)]): (String, Double) = {
    val totalRefs = numTotalRefs.map(_._1).sum
    val totalPagines = numTotalRefs.map(_._2).sum
    (fitxer, totalRefs.toDouble / totalPagines)
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

  val configuracions = List(1, 4, 10, 20)
  val documentsDir = "assets/viquipedia/documents"
  val exampleFilename = documentsDir + "/32509.xml"

  val llistaFitxers: List[(String, List[String])] = ProcessListStrings.getListOfFiles(documentsDir).map(f => (f.getName, List(f.getAbsolutePath)))

  for (i <- configuracions) {
    val (resultat, temps) = timeMeasurement(MR(llistaFitxers, MRFunctions.mappingMitjanaRefs, MRFunctions.reducingGetMitjanaRefs, i, 1))
    println(s"================== Prova amb $i actors ==================")
    println(s"El resultat és: $resultat")
    println(s"Ha trigat $temps segons")
  }
}
