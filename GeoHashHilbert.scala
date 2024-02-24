import com.tim.thesis.HashingUtils.computeBinaryHash

import scala.math.floor

object GeoHashHilbert {

  private val LNG_INTERVAL: (Double, Double) = (-180.0, 180.0)
  private val LAT_INTERVAL: (Double, Double) = (-90.0, 90.0)

  // Encode coordinate using hilbert Geohash, hilbert curve implementation based on C version found at
  // https://en.wikipedia.org/w/index.php?title=Hilbert_curve&oldid=797332503
  def encode(lng: Double, lat: Double, precision: Int, midpointShift: Option[(Int, Int)]): Seq[Boolean] = {

    val level = precision >> 1
    val dim: Long = 1L << level

    val (x, y) = coord2int(lng, lat, dim)

    val code = xy2hash(x, y, dim)

    val hash = computeBinaryHash(code, precision)

    // Midpoint shift addition in case elevation is present and midpoint should be shifted
    midpointShift.map { exists =>
      val elevationHash = computeBinaryHash(exists._2, 4)
      var replacementBits = hash.takeRight(4)

      // Default position if a collision is found between no elevation Geohash and assigned index
      if (replacementBits == elevationHash && exists._1 == 0) {
        replacementBits = Seq(true, true, true, true)
      }
      else {
        replacementBits = elevationHash
      }
      hash.take(precision - 4) ++ replacementBits
    }.getOrElse(hash)
  }

  private def coord2int(lng: Double, lat: Double, dim: Long): (Long, Long) = {
    val latY = (lat + LAT_INTERVAL._2) / 180.0 * dim
    val lngX = (lng + LNG_INTERVAL._2) / 360.0 * dim

    (math.min(dim - 1L, floor(lngX)).toLong, math.min(dim - 1, floor(latY)).toLong)
  }

  def xy2hash(x: Long, y: Long, dim: Long): BigInt = {
    def rotate(n: Long, x: Long, y: Long, rx: Long, ry: Long): (Long, Long) = {
      if (ry == 0) {
        if (rx == 1) (n - 1 - x, n - 1 - y)
        else (y, x)
      } else {
        (x, y)
      }
    }

    Iterator.iterate((dim >> 1L, x, y, BigInt(0))) {
      case (lvl, nx, ny, d) =>
        val rx = if ((nx & lvl) > 0) 1L else 0L
        val ry = if ((ny & lvl) > 0) 1L else 0L
        val newD = d + BigInt(lvl) * BigInt(lvl) * (BigInt(3) * rx ^ ry)
        val (newX, newY) = rotate(lvl, nx, ny, rx, ry)
        (lvl >> 1L, newX, newY, newD)
    }.dropWhile(_._1 > 0L).next()._4
  }
}
