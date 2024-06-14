package com.gac.x9e.pipeline

trait MessageTrait[M] {
  def readMessage(source: M)
}
