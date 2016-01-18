package org.apache.eagle.stream.dsl.execution.model

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity


private[execution] trait EntityConversion[M <: TaggedLogAPIEntity] {
  def toEntity: M
  def fromEntity (m: M)
}
