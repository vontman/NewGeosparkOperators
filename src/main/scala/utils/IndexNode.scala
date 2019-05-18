package utils

import java.util

import com.vividsolutions.jts.geom.{Envelope, Point}
import com.vividsolutions.jts.index.quadtree.NodeBase
import com.vividsolutions.jts.index.strtree.STRtree.STRtreeNode
import com.vividsolutions.jts.index.strtree.{AbstractNode, Boundable, ItemBoundable}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object IndexNode {
  def apply(node: NodeBase): IndexNode = QuadtreeNode(node)

  def apply(node: Boundable): IndexNode = RtreeNode(node)
}

trait IndexNode extends Serializable {
  def getBounds: Envelope

  def getPointsCount: Int

  def hasChildren: Boolean

  def getChildren: List[IndexNode]

  def getAllPoints: List[Point]

  def getAverageX: Double

  def getAverageY: Double
}

case class RtreeNode(node: Boundable) extends IndexNode {
  override def getBounds: Envelope = node.getBounds.asInstanceOf[Envelope]

  override def getPointsCount: Int = node.pointsCount()

  override def getChildren: List[IndexNode] = node match {
    case abstractNode: AbstractNode => {
      if (abstractNode.getLevel == 0) {
        List()
      } else {
        val allChildren = abstractNode.getChildBoundables.view.map(_.asInstanceOf[Boundable]).filter(_.pointsCount() > 0)
        val (itemBoundableChildren, abstractNodeChildren) = allChildren.partition(_.isInstanceOf[ItemBoundable])

        if (abstractNodeChildren.isEmpty) {
          List()
        }
        else if (itemBoundableChildren.nonEmpty) {
          val extraNodes = new STRtreeNode(0)
          itemBoundableChildren.foreach(extraNodes.addChildBoundable)

          IndexNode(extraNodes) :: abstractNodeChildren.map(IndexNode(_)).toList
        } else {
          abstractNodeChildren.map(IndexNode(_)).toList
        }

      }
    }
    case _: ItemBoundable =>
      List()
  }

  override def getAllPoints: List[Point] = node.getPoints.toList

  override def getAverageX: Double = node.averageX()

  override def getAverageY: Double = node.averageY()

  override def hasChildren: Boolean = node match {
    case abstractNode: AbstractNode =>
      if (abstractNode.getLevel == 0) {
        false
      } else {
        abstractNode.getChildBoundables
          .exists(_.asInstanceOf[Boundable].pointsCount() > 0)
      }
    case _: ItemBoundable =>
      false
  }
}

case class QuadtreeNode(node: NodeBase) extends IndexNode {
  override def getBounds: Envelope = node.getBounds

  override def getPointsCount: Int = node.size()

  override def getChildren: List[IndexNode] = {
    val children = node.getSubnode
      .withFilter(_ != null)
      .withFilter(!_.isEmpty)
      .withFilter(!_.isPrunable)
      .withFilter(_.size() > 0)
      .map(IndexNode(_)).toList

    if (children.nonEmpty && node.getItems.size() != 0) {
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

  override def getAllPoints: List[Point] = node.getAllItems.toList

  override def getAverageX: Double = node.averageX()

  override def getAverageY: Double = node.averageY()

  override def hasChildren: Boolean = {
    node.getSubnode
      .exists( node => node != null && !node.isEmpty && !node.isPrunable && node.size() > 0)
  }
}
