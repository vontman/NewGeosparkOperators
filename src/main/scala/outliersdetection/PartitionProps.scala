package outliersdetection

import com.vividsolutions.jts.geom.Envelope

class PartitionProps extends Serializable {

  private var _id: Int = _
  private var _envelope: Envelope = _
  private var _size: Int = _
  private var _lower: Double = 0.0
  private var _upper: Double = 0.0
  private var _neighbours: Iterable[Int] = _

  def id(id: Int): Unit = {
    this._id = id
  }

  def lower(newVal: Double): Unit = {
    _lower = newVal
  }

  def upper(newVal: Double): Unit = {
    _upper = newVal
  }

  def envelop(newVal: Envelope): Unit = {
    _envelope = newVal
  }

  def size(newVal: Int): Unit = {
    this._size = newVal
  }

  def neighbours(neighbours: Iterable[Int]): Unit = {
    this._neighbours = neighbours
  }

  def id: Int = _id

  def lower: Double = _lower

  def upper: Double = _upper

  def size: Int = this._size

  def envelop: Envelope = _envelope

  def neighbours: Iterable[Int] = _neighbours

  override def hashCode(): Int = _envelope.hashCode

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[PartitionProps] && obj.asInstanceOf[PartitionProps].envelop.equals(this.envelop)
  }

  override def toString: String = {
    s"PartitionProps(_envelope=${_envelope}, _size=${_size}, _lower=${_lower}, _upper=${_upper})"
  }
}