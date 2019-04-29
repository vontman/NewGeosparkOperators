//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.vividsolutions.jts.index.quadtree;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.util.Assert;

public class Root extends NodeBase {
    private static final Coordinate origin = new Coordinate(0.0D, 0.0D);

    public Root() {
    }

    @Override
    public Envelope getBounds() {
        Envelope env = null;
        for (NodeBase node: this.subnode) {
            if (node == null) continue;
            if (env == null)
                env = node.getBounds();
            else
                env.expandToInclude(node.getBounds());
        }
        return env;
    }

    public void insert(Envelope itemEnv, Object item) {
        int index = getSubnodeIndex(itemEnv, origin.x, origin.y);
        if (index == -1) {
            this.add(item);
        } else {
            Node node = this.subnode[index];
            if (node == null || !node.getEnvelope().contains(itemEnv)) {
                Node largerNode = Node.createExpanded(node, itemEnv);
                this.subnode[index] = largerNode;
            }

            this.insertContained(this.subnode[index], itemEnv, item);
        }
    }

    private void insertContained(Node tree, Envelope itemEnv, Object item) {
        Assert.isTrue(tree.getEnvelope().contains(itemEnv));
        boolean isZeroX = IntervalSize.isZeroWidth(itemEnv.getMinX(), itemEnv.getMaxX());
        boolean isZeroY = IntervalSize.isZeroWidth(itemEnv.getMinY(), itemEnv.getMaxY());
        Object node;
        if (!isZeroX && !isZeroY) {
            node = tree.getNode(itemEnv);
        } else {
            node = tree.find(itemEnv);
        }

        ((NodeBase)node).add(item);
    }

    protected boolean isSearchMatch(Envelope searchEnv) {
        return true;
    }
}

