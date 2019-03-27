package example

import java.util

import com.vividsolutions.jts.geom.Envelope

class PartitionProps extends Serializable {

  private var _envelope: Envelope = _
  private var _size: Int = _
  private val _neighbours: util.List[PartitionProps] = new util.ArrayList[PartitionProps]()
  private var _lower: Double = 0.0
  private var _upper: Double = 0.0

  def lower(newVal: Double): Unit = {
    _lower = newVal
  }

  def upper(newVal: Double): Unit = {
    _upper = newVal
  }

  def setEnvelop(newVal: Envelope): Unit = {
    _envelope = newVal
  }

  def setSize(newVal: Int): Unit = {
    this._size = newVal
  }

  def neighbours: util.List[PartitionProps] = _neighbours

  def lower: Double = _lower

  def upper: Double = _upper

  def size: Int = this._size

  def envelop: Envelope = _envelope


  def addNeighbour(partitionProps: PartitionProps): Boolean = {
    _neighbours.add(partitionProps)
  }

  def addAllNeighbours(allNeighbours: util.Collection[PartitionProps]): Boolean = {
    _neighbours.addAll(allNeighbours)
  }

  override def hashCode(): Int = {
    _envelope.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[PartitionProps] && obj.asInstanceOf[PartitionProps].hashCode().equals(this.hashCode())
  }
}
