package mesosphere.marathon
package raml

import java.time.{Instant, OffsetDateTime, ZoneId}

import kamon.metric.PeriodSnapshot
import kamon.metric.{Counter => KCounter, Histogram => KHistogram, MeasurementUnit => KMeasurementUnit}
import play.api.libs.json.{JsObject, Json}

trait MetricsConversion {
  lazy val zoneId = ZoneId.systemDefault()
  implicit val unitOfMeasurementRamlWriter: Writes[KMeasurementUnit, UnitOfMeasurement] = Writes { unit: KMeasurementUnit =>
    if (unit.dimension == KMeasurementUnit.Dimension.Time)
      if (unit == KMeasurementUnit.time.nanoseconds)
        TimeMeasurement("ns")
      else
        TimeMeasurement(unit.magnitude.name)
    else
      GeneralMeasurement(name = unit.name, label = unit.label)
  }

  implicit val metricsRamlWriter: Writes[PeriodSnapshot, Metrics] = Writes { snapshot =>
    val metrics = snapshot.metrics.flatMap {
      case (entity, entitySnapshot) =>
        entitySnapshot.metrics.map {
          case (metricKey, metricSnapshot) =>
            val metricName = if (entity.category == metricKey.name) entity.name else s"${entity.name}.${metricKey.name}"
            metricSnapshot match {
              case histogram: KHistogram.Snapshot =>
                (entity.category, metricName) -> Histogram(
                  count = histogram.numberOfMeasurements,
                  min = histogram.min,
                  max = histogram.max,
                  p50 = histogram.percentile(50.0),
                  p75 = histogram.percentile(75.0),
                  p98 = histogram.percentile(98.0),
                  p99 = histogram.percentile(99.0),
                  p999 = histogram.percentile(99.9),
                  mean = if (histogram.numberOfMeasurements != 0) histogram.sum.toFloat / histogram.numberOfMeasurements.toFloat else 0.0f,
                  tags = entity.tags,
                  unit = Raml.toRaml(metricKey.unitOfMeasurement)
                )
              case cs: KCounter.Snapshot =>
                (entity.category, metricName) ->
                  Counter(count = cs.count, tags = entity.tags, unit = Raml.toRaml(metricKey.unitOfMeasurement))
            }
        }
    }.groupBy(_._1._1).map {
      case (category, allMetrics) =>
        category -> allMetrics.map { case ((_, name), entityMetrics) => name -> entityMetrics }
    }

    Metrics(
      // the start zoneId could be different than the current system zone.
      start = OffsetDateTime.ofInstant(Instant.ofEpochMilli(snapshot.from.millis), zoneId),
      end = OffsetDateTime.ofInstant(Instant.ofEpochMilli(snapshot.to.millis), zoneId),
      counters = metrics.getOrElse("counter", Map.empty).collect { case (k, v: Counter) => k -> v },
      gauges = metrics.getOrElse("gauge", Map.empty).collect { case (k, v: Histogram) => k -> v },
      histograms = metrics.getOrElse("histogram", Map.empty).collect { case (k, v: Histogram) => k -> v },
      `min-max-counters` = metrics.getOrElse("min-max-counter", Map.empty).collect { case (k, v: Histogram) => k -> v },
      additionalProperties = JsObject(
        metrics.collect {
          case (name, metrics) if name != "counter" && name != "gauge" && name != "histogram" && name != "min-max-counter" =>
            name -> JsObject(metrics.collect {
              case (name, histogram: Histogram) => name -> Json.toJson(histogram)
              case (name, counter: Counter) => name -> Json.toJson(counter)
            })
        }
      )
    )
  }
}

object MetricsConversion extends MetricsConversion
