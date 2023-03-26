import akka.Done
import com.github.tototoshi.csv.*
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Concat, FileIO, Flow, Framing, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.util.ByteString

import java.io.{File, FilenameFilter}
import scala.concurrent.Future
object HumiditySensorDataStatisticsCalculator {

  def main(args: Array[String]): Unit = {
    if (args.length <= 0) println("Please provide path to the directory")
    else {
      val system = ActorSystem.create("Sensor")
      implicit val materializer = Materializer(system)

      val sensorId = "sensor-id"
      val dir = new File(getClass.getResource(args(0)).getPath)
      val files = dir.listFiles(filter)

      val bcast = Broadcast[Option[SensorStatistics]](2)

      val flow1 = Flow[Option[SensorStatistics]].fold(SensorMeasurementStats()){(measurement, stats) =>
        stats match {
          case Some(_) => SensorMeasurementStats(totalFiles = files.length, totalMeasurement = measurement.totalMeasurement + 1, failedMeasurement = measurement.failedMeasurement)
          case None => SensorMeasurementStats(totalFiles = files.length, totalMeasurement = measurement.totalMeasurement, failedMeasurement = measurement.failedMeasurement + 1)
        }
      }

      val flow2 = Flow[Option[SensorStatistics]].filter(!_.isEmpty).fold(Map.empty[String, SensorMeasurement]) { (result, data) =>
        data match {
          case Some(d) if result.isEmpty || !result.contains(d.name) =>
            result + (d.name -> SensorMeasurement(d.humidity, d.humidity, d.humidity))

          case Some(d) if result.contains(d.name) && isNumeric(d.humidity) =>
            result + (d.name -> calculateStatsForValidHumidity(result(d.name), d.humidity))

          case Some(d) if result.contains(d.name) && !isNumeric(d.humidity) =>
            result + (d.name -> calculateStatsForInvalidHumidity(result(d.name), d.humidity))

          case _ => result
        }
      }.map(m => m.toSeq.sortWith{(a, b) =>
        (a._2.avg.toIntOption, b._2.avg.toIntOption) match {
          case (Some(x), Some(y)) => y < x
          case _ => false
        }
      })

      val sources = files.map { file =>
        Source.fromIterator(() =>
          CSVReader.open(file).iterator.map {
            case List(id, humidity) if !id.toString.startsWith(sensorId) =>
              Some(SensorStatistics(id.toString, humidity.toString))
            case _ => None
          }
        )
      }

      val source = Source.combine(sources.head, Source.empty, sources.tail: _*)(Merge(_))

      val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._

        val sourceShape = builder.add(source)
        val bcastShape = builder.add(bcast)
        val flow1Shape = builder.add(flow1)
        val flow2Shape = builder.add(flow2)
        val sink1Shape = builder.add(Sink.foreach[SensorMeasurementStats](sensorMeasurementStats => {
          println(s"Num of processed files: ${sensorMeasurementStats.totalFiles}")
          println(s"Num of processed measurements: ${sensorMeasurementStats.totalMeasurement}")
          println(s"Num of failed measurements: ${sensorMeasurementStats.failedMeasurement}")
        }))

        val sink2Shape = builder.add(Sink.foreach[Seq[(String, SensorMeasurement)]](sensorMeasurement => {
          println("sensor-id,min,avg,max")
          sensorMeasurement.foreach((sensorId, reading) => {
            println(s"$sensorId,${reading.min},${reading.avg},${reading.max}")
          })
        }))

        sourceShape ~> bcastShape
        bcastShape ~> flow1Shape ~> sink1Shape
        bcastShape ~> flow2Shape ~> sink2Shape

        ClosedShape
      })
      graph.run()

    }
  }

  private def calculateStatsForInvalidHumidity(sensorStats: SensorMeasurement, humidity: String): SensorMeasurement = {
    var min, max, avg: String = ""
    if (sensorStats.min.isEmpty) {
      min = humidity
    } else {
      min = sensorStats.min
    }

    if (sensorStats.max.isEmpty) {
      max = humidity
    } else {
      max = sensorStats.max
    }
    avg = ((min.toInt + max.toInt) / 2).toString
    SensorMeasurement(min = min, max = max, avg = avg)
  }

  private def calculateStatsForValidHumidity(sensorStats: SensorMeasurement, humidity: String): SensorMeasurement = {
    var min, max, avg: String = ""
    if (sensorStats.min.equalsIgnoreCase("NaN")) {
      min = humidity
    } else {
      if (sensorStats.min.toInt > humidity.toInt) {
        min = humidity
      } else {
        min = sensorStats.min
      }
    }

    if (sensorStats.max.equalsIgnoreCase("NaN")) {
      max = humidity
    } else {
      if (sensorStats.max.toInt < humidity.toInt) {
        max = humidity
      } else {
        max = sensorStats.max
      }
    }

    avg = ((min.toInt + max.toInt) / 2).toString
    SensorMeasurement(min = min, max = max, avg = avg)
  }

  val filter = new FilenameFilter {
    val extension = ".csv"
    def accept(dir: File, name: String): Boolean = {
      name.endsWith(extension)
    }
  }

  def isNumeric(str: String): Boolean = {
    try {
      str.toInt
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  case class SensorMeasurementStats(totalFiles: Int = 0, totalMeasurement: Int = 0, failedMeasurement: Int = 0)

  case class SensorStatistics(name: String, humidity: String)

  case class SensorMeasurement(avg: String, min: String, max: String)
}