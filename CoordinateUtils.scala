import com.tim.thesis.HashingUtils._

import scala.collection.mutable.Set
import math._
import scala.collection.mutable
import scala.math.BigDecimal.RoundingMode
object CoordinateUtils {
  case class Coordinate(latitude: BigDecimal, longitude: BigDecimal, index: Int)

  private val EARTH_RADIUS_CM = 637100000

  def haversine(p1: Coordinate, p2: Coordinate): Double = {
    val lat1Rad = toRadians(p1.latitude.toDouble)
    val lon1Rad = toRadians(p1.longitude.toDouble)
    val lat2Rad = toRadians(p2.latitude.toDouble)
    val lon2Rad = toRadians(p2.longitude.toDouble)

    val latDiff = lat2Rad - lat1Rad
    val lonDiff = lon2Rad - lon1Rad

    val a = pow(sin(latDiff / 2), 2) + cos(lat1Rad) * cos(lat2Rad) * pow(sin(lonDiff / 2), 2)
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))

    EARTH_RADIUS_CM * c
  }

  def angle(p1: Coordinate, p2: Coordinate): Double = {
    val x_diff = p2.longitude - p1.longitude
    val y_diff = p2.latitude - p1.latitude
    var angle = atan2(y_diff.toDouble, x_diff.toDouble)
    if (angle < 0) {
      angle = 2 * Pi + angle
    }
    angle
  }


  // Regular geohash function using Z-level curve. Orignally was used in core algorithm, was later replaced
  // By hilbert geohash in optimisation.
  def geoEncode(coordinates: Coordinate, precision: Int, midpointShift: Option[(Int, Int)]): Seq[Boolean] = {
    val latRange = (-90.0, 90.0)
    val lonRange = (-180.0, 180.0)
    val latBits = binarySearch(coordinates.latitude, latRange, precision / 2 )
    val lonBits = binarySearch(coordinates.longitude, lonRange, precision / 2)
    val hash = interleave(latBits, lonBits)
    midpointShift.map { exists =>
      val elevationHash = computeBinaryHash(exists._2, 4)
      var replacementBits = hash.takeRight(4)
      if (replacementBits == elevationHash && exists._1 == 0) {
        replacementBits = Seq(true, true, true, true)
      }
      else {
        replacementBits = elevationHash
      }
      hash.take(precision - 4) ++ replacementBits
    }.getOrElse(hash)
  }

  private def interleave(seq1: Seq[Boolean], seq2: Seq[Boolean]): Seq[Boolean] = {
    seq1.zip(seq2).flatMap { case (b1, b2) => Seq(b1, b2) }
  }

  // Binary search algorithm used by angle and geohash
  def binarySearch(value: BigDecimal, range: (Double, Double), precision: Int): Seq[Boolean] = {
    if (precision == 0) {
      Seq.empty
    }
    else {
      val mid = (range._1 + range._2) / 2.0
      if (value > mid) true +: binarySearch(value, (mid, range._2), precision - 1)
      else false +: binarySearch(value, (range._1, mid), precision - 1)
    }
  }

  def calculateMidPoint(coordinates: Seq[Coordinate]): Coordinate = {
    val sumLat = coordinates.map(_.latitude).sum
    val sumLong = coordinates.map(_.longitude).sum
    val avgLat = sumLat / coordinates.size
    val avgLong = sumLong / coordinates.size
    roundCoordinate(Coordinate(avgLat, avgLong, 0))
  }

  // Compute diagonal length of geohash at given precision, used for midpoint shifting
  // since we need to shift atleast one geohash tile away
  def geoHashDiagonalLength(precision: Int): Double  = {
    val unevenBit = precision % 2
    val latTileSize = 180.0 / Math.pow(2, precision / 2)
    val lonTileSize = 360.0 / Math.pow(2, (precision / 2) + unevenBit)
    Math.sqrt(Math.pow(latTileSize, 2) + Math.pow(lonTileSize, 2))
  }

  // Method to compute the shifted midpoint given the original midpoint, nodes of the polyline
  // and the distance it should be shifted
  def shiftMidpoint(coordinates: Seq[Coordinate], midpoint: Coordinate, distance: Double): Coordinate = {
    if (distance == 0) midpoint
    else {
      val vector = calculateVector(coordinates.head, coordinates.last) match {
        case _ if coordinates.head == coordinates.last => calculateVector(coordinates.head, coordinates.tail.head)
        case vector                                    => vector
      }
      val normalizedVector = normalizeVector((vector._1.toDouble, vector._2.toDouble))
      val shiftedLat = midpoint.latitude + normalizedVector._1 * distance
      val shiftedLong = midpoint.longitude + normalizedVector._2 * distance
      roundCoordinate(Coordinate(shiftedLat, shiftedLong, 0))
    }
  }

  private def linearInterpolation(start: Coordinate, end: Coordinate, steps: Int): Seq[Coordinate] = {
    (0 to steps).map { i =>
      val ratio = i.toDouble / steps
      val lat = start.latitude + (end.latitude - start.latitude) * ratio
      val lon = start.longitude + (end.longitude - start.longitude) * ratio
      Coordinate(lat, lon, 0)
    }
  }

  // Helper function for LSH test to geohash a polyline with at least 8 interpolated points
  def geohashPolyline(polyline: Seq[Coordinate], precision: Int, interpolationSteps: Int): Set[Seq[Boolean]] = {
    val geohashes = mutable.Set[Seq[Boolean]]()

    // Iterate over the segments of the polyline and interpolate points
    polyline.sliding(2).foreach { segment =>
      val start = segment.head
      val end = segment.last
      val interpolatedPoints = linearInterpolation(start, end, interpolationSteps)

      // Geohash each interpolated point
      interpolatedPoints.foreach { point =>
        val hash = geoEncode(point, precision, None)
        geohashes += hash
      }
    }

    geohashes
  }

  private def normalizeVector(vector: (Double, Double)): (Double, Double) = {
    val magnitude = math.sqrt(vector._1 * vector._1 + vector._2 * vector._2)
    (vector._1 / magnitude, vector._2 / magnitude)
  }

  // Computed deltaLatitude and deltaLongitude for a polyline segment
  private def calculateVector(start: Coordinate, end: Coordinate): (BigDecimal, BigDecimal) = {
    val latDiff = end.latitude - start.latitude
    val longDiff = end.longitude - start.longitude
    (latDiff, longDiff)
  }

  // As 7 digits is supported in OSM's data model, it is useful to round to this precision when needed
  def roundCoordinate(coordinate: Coordinate): Coordinate = {
    val roundedLat = coordinate.latitude.setScale(7, RoundingMode.HALF_DOWN)
    val roundedLong = coordinate.longitude.setScale(7, RoundingMode.HALF_DOWN)
    Coordinate(roundedLat, roundedLong, 0)
  }

}
