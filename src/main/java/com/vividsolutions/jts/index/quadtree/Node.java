//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.vividsolutions.jts.index.quadtree;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.util.Assert;

public class Node extends NodeBase {
    private Envelope env;
    private double centrex;
    private double centrey;
    private int level;

    public static Node createNode(Envelope env) {
        Key key = new Key(env);
        Node node = new Node(key.getEnvelope(), key.getLevel());
        return node;
    }

    public static Node createExpanded(Node node, Envelope addEnv) {
        Envelope expandEnv = new Envelope(addEnv);
        if (node != null) {
            expandEnv.expandToInclude(node.env);
        }

        Node largerNode = createNode(expandEnv);
        if (node != null) {
            largerNode.insertNode(node);
        }

        return largerNode;
    }

    public Node(Envelope env, int level) {
        this.env = env;
        this.level = level;
        this.centrex = (env.getMinX() + env.getMaxX()) / 2.0D;
        this.centrey = (env.getMinY() + env.getMaxY()) / 2.0D;
    }

    public Envelope getEnvelope() {
        return this.env;
    }

    public int getLevel() {
        return this.level;
    }

    @Override
    public Envelope getBounds() {
        return env;
    }

    protected boolean isSearchMatch(Envelope searchEnv) {
        return this.env.intersects(searchEnv);
    }

    public Node getNode(Envelope searchEnv) {
        int subnodeIndex = getSubnodeIndex(searchEnv, this.centrex, this.centrey);
        if (subnodeIndex != -1) {
            Node node = this.getSubnode(subnodeIndex);
            return node.getNode(searchEnv);
        } else {
            return this;
        }
    }

    public NodeBase find(Envelope searchEnv) {
        int subnodeIndex = getSubnodeIndex(searchEnv, this.centrex, this.centrey);
        if (subnodeIndex == -1) {
            return this;
        } else if (this.subnode[subnodeIndex] != null) {
            Node node = this.subnode[subnodeIndex];
            return node.find(searchEnv);
        } else {
            return this;
        }
    }

    void insertNode(Node node) {
        Assert.isTrue(this.env == null || this.env.contains(node.env));
        int index = getSubnodeIndex(node.env, this.centrex, this.centrey);
        if (node.level == this.level - 1) {
            this.subnode[index] = node;
        } else {
            Node childNode = this.createSubnode(index);
            childNode.insertNode(node);
            this.subnode[index] = childNode;
        }

    }

    private Node getSubnode(int index) {
        if (this.subnode[index] == null) {
            this.subnode[index] = this.createSubnode(index);
        }

        return this.subnode[index];
    }

    private Node createSubnode(int index) {
        double minx = 0.0D;
        double maxx = 0.0D;
        double miny = 0.0D;
        double maxy = 0.0D;
        switch(index) {
            case 0:
                minx = this.env.getMinX();
                maxx = this.centrex;
                miny = this.env.getMinY();
                maxy = this.centrey;
                break;
            case 1:
                minx = this.centrex;
                maxx = this.env.getMaxX();
                miny = this.env.getMinY();
                maxy = this.centrey;
                break;
            case 2:
                minx = this.env.getMinX();
                maxx = this.centrex;
                miny = this.centrey;
                maxy = this.env.getMaxY();
                break;
            case 3:
                minx = this.centrex;
                maxx = this.env.getMaxX();
                miny = this.centrey;
                maxy = this.env.getMaxY();
        }

        Envelope sqEnv = new Envelope(minx, maxx, miny, maxy);
        Node node = new Node(sqEnv, this.level - 1);
        return node;
    }
}

