package com.quantifind.kafka.offsetapp

import java.lang.reflect.Constructor
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.quantifind.kafka.OffsetGetter
import com.quantifind.kafka.OffsetGetter.KafkaInfo
import com.quantifind.kafka.offsetapp.sqlite.SQLiteOffsetInfoReporter
import com.quantifind.sumac.validation.Required
import com.quantifind.utils.UnfilteredWebApp
import com.quantifind.utils.Utils.retry
import com.twitter.util.Time
import kafka.utils.Logging
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import org.json4s.{CustomSerializer, JInt, NoTypeHints}
import org.reflections.Reflections
import unfiltered.filter.Plan
import unfiltered.filter.request.ContextPath
import unfiltered.request.{GET, Path, Seg}
import unfiltered.response.{JsonContent, Ok, ResponseString}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.control.NonFatal

class OWArgs extends OffsetGetterArgs with UnfilteredWebApp.Arguments {
  @Required
  var retain: FiniteDuration = _

  @Required
  var refresh: FiniteDuration = _

  var dbName: String = "offsetapp"

  lazy val db = new OffsetDB(dbName)

  var pluginsArgs : String = _
}

/**
 * A webapp to look at consumers managed by kafka and their offsets.
 * User: pierre
 * Date: 1/23/14
 */
object OffsetGetterWeb extends UnfilteredWebApp[OWArgs] with Logging {

  implicit def funToRunnable(fun: () => Unit) = new Runnable() { def run() = fun() }

  def htmlRoot: String = "/offsetapp"

  val  scheduler : ScheduledExecutorService = Executors.newScheduledThreadPool(2)

  var reporters: mutable.Set[OffsetInfoReporter] = null

  def retryTask[T](fn: => T) {
    try {
      retry(3) {
        fn
      }
    } catch {
      case NonFatal(e) =>
        error("Failed to run scheduled task", e)
    }
  }

  def reportOffsets(args: OWArgs) {
    val groups = getGroups(args)
    groups.foreach {
      g =>
        val inf = getInfo(g, args).offsets.toIndexedSeq
        debug(s"reporting ${inf.size}")
        reporters.foreach( reporter => retryTask { reporter.report(inf) } )
    }
  }

  def schedule(args: OWArgs) {

    scheduler.scheduleAtFixedRate( () => { reportOffsets(args) }, 0, args.refresh.toMillis, TimeUnit.MILLISECONDS )
    scheduler.scheduleAtFixedRate( () => { reporters.foreach(reporter => retryTask({reporter.cleanupOldData()})) }, args.retain.toMillis, args.retain.toMillis, TimeUnit.MILLISECONDS )

  }

  def withOG[T](args: OWArgs)(f: OffsetGetter => T): T = {
    var og: OffsetGetter = null
    try {
      og = OffsetGetter.getInstance(args)
      f(og)
    } finally {

    }
  }

  def getInfo(group: String, args: OWArgs): KafkaInfo = withOG(args) {
    _.getInfo(group)
  }

  def getGroups(args: OWArgs) = withOG(args) {
    _.getGroups
  }

  def getActiveTopics(args: OWArgs) = withOG(args) {
    _.getActiveTopics
  }
  def getTopics(args: OWArgs) = withOG(args) {
    _.getTopics
  }

  def getTopicDetail(topic: String, args: OWArgs) = withOG(args) {
    _.getTopicDetail(topic)
  }

  def getTopicAndConsumersDetail(topic: String, args: OWArgs) = withOG(args) {
    _.getTopicAndConsumersDetail(topic)
  }

  def getClusterViz(args: OWArgs) = withOG(args) {
    _.getClusterViz
  }

  def getGroupTopicDetail(group: String, topic: String, args: OWArgs) = withOG(args) {
    _.getGroupTopicDetail(group, topic)
  }


  override def afterStop() {

    scheduler.shutdown()
  }

  class TimeSerializer extends CustomSerializer[Time](format => (
    {
      case JInt(s)=>
        Time.fromMilliseconds(s.toLong)
    },
    {
      case x: Time =>
        JInt(x.inMilliseconds)
    }
    ))

  override def setup(args: OWArgs): Plan = new Plan {
    implicit val formats = Serialization.formats(NoTypeHints) + new TimeSerializer
    args.db.maybeCreate()

    reporters = createOffsetInfoReporters(args)

    schedule(args)

    def intent: Plan.Intent = {
      case GET(Path(Seg("group" :: Nil))) =>
        JsonContent ~> ResponseString(write(getGroups(args)))
      case GET(Path(Seg("group" :: group :: Nil))) =>
        val info = getInfo(group, args)
        JsonContent ~> ResponseString(write(info)) ~> Ok
      case GET(Path(Seg("group" :: group :: topic :: Nil))) =>
        val offsets = args.db.offsetHistory(group, topic)
        JsonContent ~> ResponseString(write(offsets)) ~> Ok
      case GET(Path(Seg("topiclist" :: Nil))) =>
        JsonContent ~> ResponseString(write(getTopics(args)))
      case GET(Path(Seg("clusterlist" :: Nil))) =>
        JsonContent ~> ResponseString(write(getClusterViz(args)))
      case GET(Path(Seg("topicdetails" :: topic :: Nil))) =>
        JsonContent ~> ResponseString(write(getTopicDetail(topic, args)))
      case GET(Path(Seg("topic" :: topic :: "consumer" :: Nil))) =>
        JsonContent ~> ResponseString(write(getTopicAndConsumersDetail(topic, args)))
      case GET(ContextPath(_, Seg("group" :: group :: "topic" :: topic :: Nil))) =>
        val offsetsForTopic = getGroupTopicDetail(group, topic, args)
        JsonContent ~> ResponseString(write(offsetsForTopic)) ~> Ok
      case GET(Path(Seg("activetopics" :: Nil))) =>
        JsonContent ~> ResponseString(write(getActiveTopics(args)))
    }
  }

  def createOffsetInfoReporters(args: OWArgs) = {

    val reflections = new Reflections()

    val reportersTypes: java.util.Set[Class[_ <: OffsetInfoReporter]] = reflections.getSubTypesOf(classOf[OffsetInfoReporter])

    val reportersSet: mutable.Set[Class[_ <: OffsetInfoReporter]] = scala.collection.JavaConversions.asScalaSet(reportersTypes)

    // SQLiteOffsetInfoReporter as a main storage is instantiated explicitly outside this loop so it is filtered out
    reportersSet
      .filter(!_.equals(classOf[SQLiteOffsetInfoReporter]))
      .map((reporterType: Class[_ <: OffsetInfoReporter]) =>  createReporterInstance(reporterType, args.pluginsArgs))
      .+(new SQLiteOffsetInfoReporter(argHolder.db, args))
  }

  def createReporterInstance(reporterClass: Class[_ <: OffsetInfoReporter], rawArgs: String): OffsetInfoReporter = {
    val constructor: Constructor[_ <: OffsetInfoReporter] = reporterClass.getConstructor(classOf[String])
    constructor.newInstance(rawArgs)
  }
}
