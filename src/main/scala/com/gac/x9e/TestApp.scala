package com.gac.x9e

import com.gac.x9e.model.TripSession


import scala.collection.mutable.ArrayBuffer

object TestApp extends App {
  val tripResult: ArrayBuffer[Int] = ArrayBuffer[Int]()
    def test() = {
      val a: Option[Int] = Some(1)
      val t = for {
        integerA <- a
      } {
        tripResult.append(integerA)
        tripResult.foreach(println(_))
        println("Hello World")
      }
    }

  test()

}
