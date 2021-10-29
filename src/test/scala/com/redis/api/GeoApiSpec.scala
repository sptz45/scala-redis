package com.redis.api

import com.redis.GeoRadiusMember
import com.redis.common.IntSpec
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

trait GeoApiSpec extends AnyFunSpec
  with Matchers
  with IntSpec {

  override protected def r: BaseApi with StringApi with GeoApi with AutoCloseable

  geoadd()
  geopos()
  geohash()
  geodist()
  georadius()
  georadiusbymember()

  protected def geoadd(): Unit = {
  describe("geoadd") {
    it("should add values with their coordinates and return the added quantity") {
      val out = r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      out should  be(Some(2))
    }
    it("should not add a value twice") {
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      out should  be(Some(0))
    }
  }
  }

  protected def geopos(): Unit = {
  describe("geopos") {
    it("should correctly expose coordinates of requested members"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.geopos("Sicily", List("Catania", "Palermo", "testFail", "TestTest"))
      out should equal(
        Some(List(
          Some(List(Some("15.08726745843887329"), Some("37.50266842333162032"))),
          Some(List(Some("13.36138933897018433"), Some("38.11555639549629859"))),
          None,
          None))
      )
    }
    it("correctly handle empty requested members"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.geopos("Sicily", List("testFail", "TestTest"))
      out should equal(
        Some(List(
          None,
          None))
      )
    }
  }
  }

  protected def geohash(): Unit = {
  describe("geohash") {
    it("should expose correctly the stored hash"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.geohash[String]("Sicily", List("Palermo",  "Catania"))
      out should be(Some(List(Some("sqc8b49rny0"), Some("sqdtr74hyu0"))))
    }
    it("should retrieve nil for absent members"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.geohash[String]("Sicily", List("unknown"))
      out should be(Some(List(None)))
    }
  }
  }

  protected def geodist(): Unit = {
  describe("geodist"){
    it("should correctly compute the distance between two objects, defaulting to meters"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.geodist("Sicily", "Palermo", "Catania", None)
      out.isDefined should be(true)
      out.get should startWith("166274.151") //precision handling ???
    }
    it("should correctly compute the distance between two objects in kilometers"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.geodist("Sicily", "Palermo", "Catania", Some("km"))
      out.isDefined should be(true)
      out.get should startWith("166.274") //precision handling ???
    }
    it("should return an empty value if a member can't be found"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.geodist("Sicily", "Palermo", "testfail", Some("km"))
      out.isDefined should be(false)
    }
  }
  }

  protected def georadius(): Unit = {
  describe("georadius"){
    it("should correctly retrieve members in the radius with their hash and dist"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.georadius("Sicily", "15", "37", "200", "km", false, true, true, None, None, None, None)
      out should equal(
        Some(List(
          Some(GeoRadiusMember(Some("Palermo"),Some(3479099956230698L),Some("190.4424"),None)),
          Some(GeoRadiusMember(Some("Catania"),Some(3479447370796909L),Some("56.4413"),None))))
      )
    }
    it("should correctly retrieve members in the radius with their name only"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.georadius("Sicily", "15", "37", "200", "km", false, false, false, None, None, None, None)
      out should equal(
        Some(List(
          Some(GeoRadiusMember(Some("Palermo"),None,None,None)),
          Some(GeoRadiusMember(Some("Catania"),None,None,None))))
      )
    }
    it("should correctly retrieve members in the radius with their hash, dist and coords"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.georadius("Sicily", "15", "37", "200", "km", true, true, true, None, None, None, None)
      out should equal(
        Some(List(
          Some(GeoRadiusMember(Some("Palermo"),Some(3479099956230698L),Some("190.4424"),Some(("13.36138933897018433","38.11555639549629859")))),
          Some(GeoRadiusMember(Some("Catania"),Some(3479447370796909L),Some("56.4413"), Some(("15.08726745843887329","37.50266842333162032"))))
        ))
      )
    }
    it("should correctly retrieve members in the radius with their hash, dist and coords in ascending order"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.georadius("Sicily", "15", "37", "200", "km", true, true, true, None, Some("ASC"), None, None)
      out should equal(
        Some(List(
          Some(GeoRadiusMember(Some("Catania"),Some(3479447370796909L),Some("56.4413"), Some(("15.08726745843887329","37.50266842333162032")))),
          Some(GeoRadiusMember(Some("Palermo"),Some(3479099956230698L),Some("190.4424"),Some(("13.36138933897018433","38.11555639549629859"))))
        ))
      )
    }
    it("should correctly retrieve members in the radius with their hash, dist and coords in descending order"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.georadius("Sicily", "15", "37", "200", "km", true, true, true, None, Some("DESC"), None, None)
      out should equal(
        Some(List(
          Some(GeoRadiusMember(Some("Palermo"),Some(3479099956230698L),Some("190.4424"),Some(("13.36138933897018433","38.11555639549629859")))),
          Some(GeoRadiusMember(Some("Catania"),Some(3479447370796909L),Some("56.4413"), Some(("15.08726745843887329","37.50266842333162032"))))
        ))
      )
    }
    it("should correctly limit the returned members in the radius"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")))
      val out = r.georadius("Sicily", "15", "37", "200", "km", false, false, false, Some(1), None, None, None)
      out should equal(
        Some(List(
          Some(GeoRadiusMember(Some("Catania"),None,None,None))
        ))
      )
    }
  }
  }

  protected def georadiusbymember(): Unit = {
  describe("georadiusbymember"){
    it("should correctly retrieve members in the radius with their hash and dist"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania"), ("13.583333", "37.316667", "Agrigento")))
      val out = r.georadiusbymember("Sicily", "Agrigento", "100", "km", false, true, true, None, None, None, None)
      val act = out.get
      act.size should equal(2)
      act should contain (Some(GeoRadiusMember(Some("Agrigento"),Some(3479030013248308L),Some("0.0000"),None)))
      act should contain (Some(GeoRadiusMember(Some("Palermo"),Some(3479099956230698L),Some("90.9778"),None)))
    }
    it("should correctly retrieve members in the radius with their name only"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania"), ("13.583333", "37.316667", "Agrigento")))
      val out = r.georadiusbymember("Sicily", "Agrigento", "100", "km", false, false, false, None, None, None, None)
      val act = out.get
      act.size should equal(2)
      act should contain (Some(GeoRadiusMember(Some("Agrigento"),None,None,None)))
      act should contain (Some(GeoRadiusMember(Some("Palermo"),None,None,None)))
    }
    it("should correctly retrieve members in the radius with their hash, dist and coords"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania"), ("13.583333", "37.316667", "Agrigento")))
      val out = r.georadiusbymember("Sicily", "Agrigento", "100", "km", true, true, true, None, None, None, None)
      val act = out.get
      act.size should equal(2)
      act should contain(Some(GeoRadiusMember(Some("Agrigento"),Some(3479030013248308L),Some("0.0000"),Some(("13.5833314061164856","37.31666804993816555")))))
      act should contain(Some(GeoRadiusMember(Some("Palermo"),Some(3479099956230698L),Some("90.9778"),Some(("13.36138933897018433","38.11555639549629859")))))
    }
    it("should correctly retrieve members in the radius with their hash, dist and coords in ascending order"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania"), ("13.583333", "37.316667", "Agrigento")))
      val out = r.georadiusbymember("Sicily", "Agrigento", "100", "km", true, true, true, None, Some("ASC"), None, None)
      val act = out.get
      act.size should equal(2)
      act.head should equal(Some(GeoRadiusMember(Some("Agrigento"),Some(3479030013248308L),Some("0.0000"),Some(("13.5833314061164856","37.31666804993816555")))))
      act.tail.head should equal(Some(GeoRadiusMember(Some("Palermo"),Some(3479099956230698L),Some("90.9778"),Some(("13.36138933897018433","38.11555639549629859")))))
    }
    it("should correctly retrieve members in the radius with their hash, dist and coords in descending order"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania"), ("13.583333", "37.316667", "Agrigento")))
      val out = r.georadiusbymember("Sicily", "Agrigento", "100", "km", true, true, true, None, Some("DESC"), None, None)
      val act = out.get
      act.size should equal(2)
      act.head should equal(Some(GeoRadiusMember(Some("Palermo"),Some(3479099956230698L),Some("90.9778"),Some(("13.36138933897018433","38.11555639549629859")))))
      act.tail.head should equal(Some(GeoRadiusMember(Some("Agrigento"),Some(3479030013248308L),Some("0.0000"),Some(("13.5833314061164856","37.31666804993816555")))))
    }
    it("should correctly limit the returned members in the radius"){
      r.geoadd("Sicily", Seq(("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania"), ("13.583333", "37.316667", "Agrigento")))
      val out = r.georadiusbymember("Sicily", "Agrigento", "100", "km", false, false, false, Some(1), None, None, None)
      val act = out.get
      act.size should equal(1)
      act should contain(Some(GeoRadiusMember(Some("Agrigento"),None,None,None)))
    }
  }
  }
}
