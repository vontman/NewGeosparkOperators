package example

import java.util

import com.vividsolutions.jts.geom.Envelope

class PartitionProps extends Serializable {

  private var _envelope: Envelope = _
  private val _subPartitions: util.ArrayList[PartitionProps] = new util.ArrayList[PartitionProps]()
  private var _size: Int = _
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

  def lower: Double = _lower

  def upper: Double = _upper

  def size: Int = this._size

  def envelop: Envelope = _envelope

  def subPartitions: util.List[PartitionProps] = _subPartitions

  def addSubPartition(subPartition: PartitionProps): Boolean = _subPartitions.add(subPartition)


  override def hashCode(): Int = _envelope.hashCode


  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[PartitionProps] && obj.asInstanceOf[PartitionProps].hashCode().equals(this.hashCode())
  }
}
