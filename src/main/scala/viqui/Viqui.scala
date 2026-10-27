package edu.udg.pda
package viqui

import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}

import mapreduce.*
import utils.ProcessListStrings
import utils.ViquipediaParse

// L'Actor MapReduce és polimòrfic amb els tipus de les claus valor de l'entrada [K1,V1], la clau i valor intermitjos [k2,v2]
// i la clau i valor finals [K2,V3].
// - input és el paràmetre d'entrada (compte perquè depedent de la mida pot ser un problema)
// - mapping és la funció dels mappers
// - reducing és la funció dels reducers
class MapReduce[K1,V1,K2,V2,V3](
                                 input:List[(K1,List[V1])],
                                 mapping:(K1,List[V1]) => List[(K2,V2)],
                                 reducing:(K2,List[V2])=> (K2,V3)) extends Actor {


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
  var client:ActorRef = null


  def receive: Receive = {
    // case msg =>
    //   println(s"MapReduce received: $msg")

    // En rebre el missatge MapReduceCompute engeguem el procés.
    case MapReduceCompute() =>
      println("Hem rebut lencarrec")
      client = sender() // Ens apuntem qui ens ha fet l'encàrrec per enviar-li el missatge més tard.

      // farem un mapper per parella (K1,List[V1]) de l'input

      nmappers = input.length

      println("Going to create MAPPERS!!")

      // Al crear actors a dins d'altres actors, enlloc de crear el ActorSystem utilitzarem context i així es va
      // organitzant la jerarquia d'actors.
      // D'altra banda, quan els actors que creem tenen un contructor amb paràmetre, no passem el "tipus" de l'actor i prou
      // a Props sino que creem l'actor amb els paràmetres que necessita. En aquest cas, l'Actor mapping és paramètric en tipus
      // i necessita com a paràmetre una funció de mapping.

      val mappers: Seq[ActorRef] = for (i <- 0 until nmappers) yield {
        //   context.actorOf(
        //     Props(classOf[Mapper[K1,V1,K2,V2]], mapping),
        //     name = "mapper" + i
        //   )
        context.actorOf(Props(new Mapper(mapping)), "mapper" + i)
      }
      // No és necessari passar els tipus K1,V1, ... ja que els infereix SCALA pel paràmetre mapping
      // D'altra banda compte pq  "0 until n" és el mateix que "0 to n-1".
      // val mappers = for (i <- 0 to nmappers-1) yield {
      //      context.actorOf(Props(new Mapper[K1,V1,K2,V2](mapping)), "mapper"+i)
      // }

      // Posar les anotacions de tipus aquí no és necessari però ajuda a llegir que
      // a cada mapper li enviem una clau de tipus K1 i una llista de valors de tipus V1
      for(i<- 0 until nmappers) mappers(i) ! toMapper(input(i)._1:K1, input(i)._2: List[V1])
      // Per tant, alternativament...
      // for(i<- 0 until nmappers) mappers(i) ! toMapper(input(i)._1, input(i)._2)

      // Necessitem controlar quant s'han acabat tots els mappers per poder llençar els reducers després...
      mappersPendents = nmappers

      println("All sent to Mappers, now start listening...")


    // Anem rebent les respostes dels mappers i les agrupem al diccionari per clau.
    // Tornem a necessitar anotar els tipus del paràmetre que reb fromMapper tal com ho hem fet
    // o be en el codi comentat al generar les tuples.

    case fromMapper(list_clau_valor:List[(K2,V2)]) =>
      println(s"MapReduce ${self.path.name} received input ${list_clau_valor.length}")
      for ((clau, valor) <- list_clau_valor)
        dict += (clau -> (valor :: dict(clau)))

      mappersPendents -= 1

      // Quan ja hem rebut tots els missatges dels mappers:
      if (mappersPendents==0)
      {
        println("All mappers finished")
        // creem els reducers, tants com entrades al diccionari; fixeu-vos de nou que fem servir context i fem el new
        // pel constructor del Reducer amb paràmetres
        nreducers = dict.size
        reducersPendents = nreducers // actualitzem els reducers pendents
        val reducers = for (i <- 0 until nreducers) yield
          context.actorOf(Props(new Reducer(reducing)), "reducer"+i)
        // No cal anotar els tipus ja que els infereix de la funció reducing
        //context.actorOf(Props(new Reducer[K2,V2,V3](reducing)), "reducer"+i)

        // Ara enviem a cada reducer una clau de tipus V2 i una llista de valors de tipus K2. Les anotacions de tipus
        // no caldrien perquè ja sabem de quin tipus és dict, però ens ajuden a documentar.
        for ((i,(key:K2, lvalue:List[V2])) <-  (0 until nreducers) zip dict)
          reducers(i) ! toReducer(key, lvalue)
        println("All sent to Reducers")
      }


    // A mesura que anem rebent respostes del reducer, tuples (K2, V3), les anem afegint al Map del resultatfinal i
    // descomptem reducers pendents. Tornem a necessitar anotar el tipus.
    case fromReducer(entradaDictionari:(K2,V3)) =>
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

object Viqui extends App {

  // donat un bloc de codi de tipus genèric, retorna una tupla amb el retorn del codi i el temps que ha trigat en executar-se
  def timeMeasurement[A](block: => A): (A, Double) = {
    val t0 = System.currentTimeMillis()
    val retorn = block
    val t1 = System.currentTimeMillis()
    (retorn, t1 - t0)
  }

  val documentsDir = "assets/viquipedia/documents"
  val exampleFilename = documentsDir + "/32509.xml"

  def testParse: ViquipediaParse.ResultViquipediaParsing = ViquipediaParse.parseViquipediaFile(exampleFilename)

  println(testParse)

  /*
  mapreduce(documents, mapping, reducing)

  // EX 1. Useu el MapReduce per saber quant ha gastat cada persona
  def mapping(supermercat: String, clients: List[(String, Double, String)]): List[(String, Double)] = {
    for (client <- clients) yield (client._1, client._2) // itera la llista de clients del supermercat, i retorna una tupla amb el nom d'un client i el que ha gastat
  }

  def reducing(client: String, imports: List[Double]): (String, Double) = {
    (client, imports.sum) // rep un client i una llista de despeses seves en tots els supermercats, retorna una tupla amb el mateix nom i la suma dels imports
  }
  */
}
