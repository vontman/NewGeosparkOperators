package gnn

import com.vividsolutions.jts.geom.{Envelope, Point}
import com.vividsolutions.jts.index.quadtree.NodeBase
import com.vividsolutions.jts.index.strtree.{AbstractNode, Boundable, ItemBoundable}

import scala.collection.JavaConversions._

object IndexNode {
  def apply(node: NodeBase): IndexNode = QuadtreeNode(node)
  def apply(node: Boundable): IndexNode = RtreeNode(node)
}

trait IndexNode extends Serializable {
  def getBounds: Envelope
  def getPointsCount: Int
  def getChildren: List[IndexNode]
  def getAverageX: Double
  def getAverageY: Double
}

case class RtreeNode(node: Boundable) extends IndexNode{
  override def getBounds: Envelope = node.getBounds.asInstanceOf[Envelope]

  override def getPointsCount: Int = node.pointsCount()

  override def getChildren: List[IndexNode] = node match {
    case abstractNode: AbstractNode =>
      if (abstractNode.getLevel == 0) {
        List(IndexNode(abstractNode))
      } else {
        abstractNode.getChildBoundables
          .view
          .map(_.asInstanceOf[Boundable])
          .withFilter(_.pointsCount() > 0)
          .map(IndexNode(_))
          .toList
      }
    case item: ItemBoundable =>
      List(IndexNode(item))
  }

  override def getAverageX: Double = node.averageX()

  override def getAverageY: Double = node.averageY()
}

case class QuadtreeNode(node: NodeBase) extends IndexNode{
  override def getBounds: Envelope = node.getBounds

  override def getPointsCount: Int = node.size()

  override def getChildren: List[IndexNode] = {
    val children = node.getChildren
      .withFilter(_ != null)
      .withFilter(!_.isEmpty)
      .withFilter(!_.isPrunable)
      .withFilter(_.size() > 0)
      .map(IndexNode(_)).toList

    if (node.getItems.size() != 0) {
      val env = {
        val env = new Envelope()
        val initPoint = node.getItems.head.asInstanceOf[Point]
        env.init(initPoint.getCoordinate)
        node.getItems.foreach(p => env.expandToInclude(p.asInstanceOf[Point].getCoordinate))
        env
      }

      val tmp = new com.vividsolutions.jts.index.quadtree.Node(env, 1)
      tmp.setItems(node.getItems)
      IndexNode(tmp) :: children
    } else {
      children
    }
  }

  override def getAverageX: Double = node.averageX()

  override def getAverageY: Double = node.averageY()
}
