d3.biHiSankey = function () {
  "use strict";

  var biHiSankey = {},
    nodeWidth = 24,
    nodeSpacing = 8,
    linkSpacing = 5,
    arrowheadScaleFactor = 0, // Specifies the proportion of a link's stroke width to be allowed for the marker at the end of the link.
    size = [1, 1], // default to one pixel by one pixel
    nodes = [],
    nodeMap = {},
    parentNodes = [],
    leafNodes = [],
    links = [],
    xScaleFactor = 1,
    yScaleFactor = 1,
    defaultLinkCurvature = 0.5;

  function center(node) {
    return node.y + node.height / 2;
  }

  function value(link) {
    return link.value;
  }

  function initializeNodeArrayProperties(node) {
    node.sourceLinks = [];
    node.rightLinks = [];
    node.targetLinks = [];
    node.leftLinks = [];
    node.connectedNodes = [];
    node.children = [];
    node.ancestors = [];
  }
  // generates the nodeMap {"1": <node1>, "2": <node2>}
  // and initializes the array properties of each node
  function initializeNodeMap() {
    nodes.forEach(function (node) {
      nodeMap[node.id] = node;
      initializeNodeArrayProperties(node);
    });
  }

  function computeLeafNodes() {
    leafNodes = nodes.filter(function (node) {
      return !node.children.length;
    });
  }

  function computeParentNodes() {
    parentNodes = nodes.filter(function (node) {
      return node.children.length;
    });
  }

  function addAncestorsToChildren(node) {
    node.children.forEach(function (child) {
      child.ancestors = child.ancestors.concat(this.ancestors.concat([this]));
      addAncestorsToChildren(child);
    }, node);
  }

  // generate hierarchical connections between parent and child nodes
  function computeNodeHierarchy() {
    var parent,
        rootNodes = [];

    nodes.forEach(function (node) {
      parent = nodeMap[node.parent];
      if (parent) {
        node.parent = parent;
        parent.children.push(node);
      } else {
        node.parent = null;
        rootNodes.push(node);
      }
    });

    computeLeafNodes();
    computeParentNodes();

    rootNodes.forEach(function (rNode) {
      addAncestorsToChildren(rNode);
    });
  }

  // Populate the sourceLinks and targetLinks for each node.
  function computeNodeLinks() {
    var sourceNode, targetNode;
    links.forEach(function (link) {
      sourceNode = nodeMap[link.source] || link.source;
      targetNode = nodeMap[link.target] || link.target;
      link.id = link.source + '-' + link.target;
      link.source = sourceNode;
      link.target = targetNode;
      sourceNode.sourceLinks.push(link);
      targetNode.targetLinks.push(link);
    });
  }

  function visible(linkCollection) {
    return linkCollection.filter(function (link) {
      return link.source.state === "collapsed" && link.target.state === "collapsed";
    });
  }

  // When child nodes are collapsed into their parents (or higher ancestors)
  // the links between the child nodes should be represented by links
  // between the containing ancestors. This function adds those extra links.
  function computeAncestorLinks() {
    // Leaf nodes are never parents of other nodes
    // Duplicate source and target links between a leaf node and another leaf node
    // and add to the leaf nodes' parents
    leafNodes.forEach(function (leafNode) {
      leafNode.sourceLinks.forEach(function (sourceLink) {
        var ancestorTargets,
        target = sourceLink.target;
        if (leafNodes.indexOf(target) >= 0) {
          ancestorTargets = target.ancestors.filter(function (tAncestor) {
            return leafNode.ancestors.indexOf(tAncestor) < 0;
          });
          ancestorTargets.forEach(function (ancestorTarget) {
            var ancestorLink = { source: leafNode,
                                target: ancestorTarget,
                                value: sourceLink.value,
                                id: leafNode.id + "-" + ancestorTarget.id };

            leafNode.sourceLinks.push(ancestorLink);
            ancestorTarget.targetLinks.push(ancestorLink);
            links.push(ancestorLink);
          });
        }
      });

      leafNode.targetLinks.forEach(function (targetLink) {
        var ancestorSources, source = targetLink.source;
        if (leafNodes.indexOf(source) >= 0) {
          ancestorSources = source.ancestors.filter(function (sAncestor) {
            return leafNode.ancestors.indexOf(sAncestor) < 0;
          });
          ancestorSources.forEach(function (ancestorSource) {
            var ancestorLink = { source: ancestorSource,
                                target: leafNode,
                                value: targetLink.value,
                                id: ancestorSource.id + "-" + leafNode.id };
            ancestorSource.sourceLinks.push(ancestorLink);
            leafNode.targetLinks.push(ancestorLink);
            links.push(ancestorLink);
          });
        }
      });
    });

    // Add links between parents (for when both parents are in collapsed state)
    parentNodes.forEach(function (parentNode) {
      parentNode.sourceLinks.forEach(function (sourceLink) {
        var ancestorTargets, target = sourceLink.target;
        if (leafNodes.indexOf(target) >= 0) {
          ancestorTargets = target.ancestors.filter(function (tAncestor) {
            return parentNode.ancestors.indexOf(tAncestor) < 0;
          });
          ancestorTargets.forEach(function (ancestorTarget) {
            var ancestorLink = { source: parentNode,
                                target: ancestorTarget,
                                value: sourceLink.value,
                                id: parentNode.id + "-" + ancestorTarget.id };

            parentNode.sourceLinks.push(ancestorLink);
            ancestorTarget.targetLinks.push(ancestorLink);
            links.push(ancestorLink);
          });
        }
      });
    });
  }

  // To reduce clutter in the diagram merge links that are from the
  // same source to the same target by creating a new link
  // with a value equal to the sum of the values of the merged links
  function mergeLinks() {
    var linkGroups = d3.nest()
      .key(function (link) { return link.source.id + "->" + link.target.id; })
      .entries(links)
      .map(function (object) { return object.values; });

    links = linkGroups.map(function (linkGroup) {
      return linkGroup.reduce(function (previousLink, currentLink) {
        return {
          "source": previousLink.source,
          "target": previousLink.target,
          "id": d3.min([previousLink.id, currentLink.id]),
          "value": previousLink.value + currentLink.value
        };
      });
    });
  }

  function nodeHeight(sideLinks) {
    var spacing = Math.max(sideLinks.length - 1, 0) * linkSpacing,
        scaledValueSum = d3.sum(sideLinks, value) * yScaleFactor;
    return scaledValueSum + spacing;
  }

  // Compute the value of each node by summing the associated links.
  // Compute the number of spaces between the links
  // Compute the number of source links for later decrementing
  function computeNodeValues() {
    nodes.forEach(function (node) {
      node.value = Math.max(
        d3.sum(node.leftLinks, value),
        d3.sum(node.rightLinks, value)
      );
      node.netFlow = d3.sum(visible(node.targetLinks), value) - d3.sum(visible(node.sourceLinks), value);
      node.height = Math.max(nodeHeight(visible(node.leftLinks)), nodeHeight(visible(node.rightLinks)));
      node.linkSpaceCount = Math.max(Math.max(node.leftLinks.length, node.rightLinks.length) - 1, 0);
    });
  }

  function computeConnectedNodes() {
    var sourceNode, targetNode;
    links.forEach(function (link) {
      sourceNode = link.source;
      targetNode = link.target;
      if (sourceNode.connectedNodes.indexOf(targetNode) < 0) {
        sourceNode.connectedNodes.push(targetNode);
      }
      if (targetNode.connectedNodes.indexOf(sourceNode) < 0) {
        targetNode.connectedNodes.push(sourceNode);
      }
    });
  }

  function sourceAndTargetNodesWithSameX() {
    var nodeArray = [];
    links.filter(function (link) {
      return link.target.x === link.source.x;
    }).forEach(function (link) {
      if (nodeArray.indexOf(link.target) < 0) {
        nodeArray.push(link.target);
      }
    });
    return nodeArray;
  }

  function compressInXDirection() {
    var connectedNodesXPositions,
        nodesByXPosition = d3.nest()
          .key(function (node) { return node.x; })
          .sortKeys(d3.ascending)
          .entries(nodes)
          .map(function (object) { return object.values; });

    nodesByXPosition.forEach(function (xnodes) {
      xnodes.forEach(function (node) {
        connectedNodesXPositions = node.connectedNodes.map(function (connectedNode) {
          return connectedNode.x;
        });
        // keep decrementing the x value of the node
        // unless it would have the same x value as one of its source or target nodes
        // or node.x is already 0
        while (node.x > 0 && connectedNodesXPositions.indexOf(node.x - 1) < 0) {
          node.x -= 1;
        }
      });
    });
  }

  function scaleNodeXPositions() {
    var minX = d3.min(nodes, function (node) { return node.x; }),
        maxX = d3.max(nodes, function (node) { return node.x; }) - minX;
    xScaleFactor = (size[0] - nodeWidth) / maxX;

    nodes.forEach(function (node) {
      node.x *= xScaleFactor;
    });
  }

  function computeNodeXPositions() {
    var remainingNodes = nodes,
        nextNodes,
        x = 0,
        addToNextNodes = function (link) {
          if (nextNodes.indexOf(link.target) < 0 && link.target.x === this.x) {
            nextNodes.push(link.target);
          }
        },
        setValues = function (node) {
          node.x = x;
          node.width = nodeWidth;
          node.sourceLinks.forEach(addToNextNodes, node);
        };

    while (remainingNodes.length) {
      nextNodes = [];
      remainingNodes.forEach(setValues);
      if (nextNodes.length) {
        remainingNodes = nextNodes;
      } else {
        remainingNodes = sourceAndTargetNodesWithSameX();
      }
      x += 1;
    }

    compressInXDirection();
    scaleNodeXPositions();
  }

  function computeLeftAndRightLinks() {
    var source, target;
    nodes.forEach(function (node) {
      node.rightLinks = [];
      node.leftLinks = [];
    });
    links.forEach(function (link) {
      source = link.source;
      target = link.target;
      if (source.x < target.x) {
        source.rightLinks.push(link);
        target.leftLinks.push(link);
        link.direction = 1;
      } else {
        source.leftLinks.push(link);
        target.rightLinks.push(link);
        link.direction = -1;
      }
    });
  }

  function adjustTop(adjustment) {
    nodes.forEach(function (node) {
      node.y -= adjustment;
    });
  }

  function computeNodeYPositions(iterations) {
    var minY,
        alpha,
        nodesByXPosition = d3.nest()
          .key(function (node) { return node.x; })
          .sortKeys(d3.ascending)
          .entries(nodes)
          .map(function (object) { return object.values; });

    function calculateYScaleFactor() {
      var linkSpacesCount, nodeValueSum, discretionaryY;
      yScaleFactor = d3.min(nodesByXPosition, function (nodes) {
        linkSpacesCount = d3.sum(nodes, function (node) {
          return node.linkSpaceCount;
        });
        nodeValueSum = d3.sum(nodes, function (node) {
          return node.value;
        });
        discretionaryY = (size[1]
                        - (nodes.length - 1) * nodeSpacing
                        - linkSpacesCount * linkSpacing);

        return  discretionaryY / nodeValueSum;
      });

      // Fat links are those with lengths less than about 4 times their heights
      // Fat links don't bend well
      // Test that yScaleFactor is not so big that it causes "fat" links; adjust yScaleFactor accordingly
      links.forEach(function (link) {
        var linkLength = Math.abs(link.source.x - link.target.x),
            linkHeight = link.value * yScaleFactor;
        if (linkLength / linkHeight < 4) {
          yScaleFactor = 0.25 * linkLength / link.value;
        }
      });
    }

    function initializeNodeYPosition() {
      nodesByXPosition.forEach(function (nodes) {
        nodes.forEach(function (node, i) {
          node.y = i;
          node.heightAllowance = node.value * yScaleFactor + linkSpacing * node.linkSpaceCount;
        });
      });
    }

    function calculateLinkThickness() {
      links.forEach(function (link) {
        link.thickness = link.value * yScaleFactor;
      });
    }

    function relaxLeftToRight(alpha) {
      function weightedSource(link) {
        return center(link.source) * link.value;
      }

      nodesByXPosition.forEach(function (nodes) {
        nodes.forEach(function (node) {
          if (node.rightLinks.length) {
            var y = d3.sum(node.rightLinks, weightedSource) / d3.sum(node.rightLinks, value);
            node.y += (y - center(node)) * alpha;
          }
        });
      });
    }

    function relaxRightToLeft(alpha) {
      function weightedTarget(link) {
        return center(link.target) * link.value;
      }

      nodesByXPosition.slice().reverse().forEach(function (nodes) {
        nodes.forEach(function (node) {
          if (node.leftLinks.length) {
            var y = d3.sum(node.leftLinks, weightedTarget) / d3.sum(node.leftLinks, value);
            node.y += (y - center(node)) * alpha;
          }
        });
      });
    }

    function resolveCollisions() {
      function ascendingYPosition(a, b) {
        return a.y - b.y;
      }

      nodesByXPosition.forEach(function (nodes) {
        var node,
            dy,
            y0 = 0,
            n = nodes.length,
            i;

        nodes.sort(ascendingYPosition);

        // Push any overlapping nodes down.
        for (i = 0; i < n; ++i) {
          node = nodes[i];
          dy = y0 - node.y;
          if (dy > 0) {
            node.y += dy;
          }
          y0 = node.y + node.heightAllowance + nodeSpacing;
        }

        // If the bottommost node goes outside the bounds, push it back up.
        dy = y0 - nodeSpacing - size[1];
        if (dy > 0) {
          node.y -= dy;
          y0 = node.y;

          // Push any overlapping nodes back up.
          for (i = n - 2; i >= 0; --i) {
            node = nodes[i];
            dy = node.y + node.heightAllowance + nodeSpacing - y0;
            if (dy > 0) {
              node.y -= dy;
            }
            y0 = node.y;
          }
        }
      });
    }

    calculateYScaleFactor();
    initializeNodeYPosition();
    calculateLinkThickness();
    resolveCollisions();

    for (alpha = 1; iterations > 0; --iterations) {
      alpha *= 0.99;
      relaxRightToLeft(alpha);
      resolveCollisions();
      relaxLeftToRight(alpha);
      resolveCollisions();
    }

    minY = d3.min(nodes, function (node) { return node.y; });
    adjustTop(minY);
  }

  function computeLinkYPositions() {

    function ascendingLeftNodeYPosition(a, b) {
      var aLeftNode = (a.direction > 0) ? a.source : a.target,
          bLeftNode = (b.direction > 0) ? b.source : b.target;
      return aLeftNode.y - bLeftNode.y;
    }

    function ascendingRightNodeYPosition(a, b) {
      var aRightNode = (a.direction > 0) ? a.target : a.source,
          bRightNode = (b.direction > 0) ? b.target : b.source;
      return aRightNode.y - bRightNode.y;
    }

    nodes.forEach(function (node) {
      node.rightLinks.sort(ascendingRightNodeYPosition);
      node.leftLinks.sort(ascendingLeftNodeYPosition);
    });

    nodes.forEach(function (node) {
      var rightY = 0, leftY = 0;

      node.rightLinks.forEach(function (link) {
        if (link.direction > 0) {
          link.sourceY = rightY;
        }
        else {
          link.targetY = rightY;
        }
      });

      node.leftLinks.forEach(function (link) {
        if (link.direction < 0) {
          link.sourceY = leftY;
        }
        else {
          link.targetY = leftY;
        }
      });

    });
  }


  biHiSankey.arrowheadScaleFactor = function (_) {
    if (!arguments.length) { return arrowheadScaleFactor; }
    arrowheadScaleFactor = +_;
    return biHiSankey;
  };

  biHiSankey.collapsedNodes = function () {
    return nodes.filter(function (node) { return node.state === "collapsed"; });
  };

  biHiSankey.connected = function (nodeA, nodeB) {
    return nodeA.connectedNodes.indexOf(nodeB) >= 0;
  };

  biHiSankey.expandedNodes = function () {
    return nodes.filter(function (node) { return node.state === "expanded"; });
  };

  biHiSankey.layout = function (iterations) {
    computeNodeXPositions();
    computeLeftAndRightLinks();
    computeNodeValues();
    computeNodeYPositions(iterations);
    computeNodeValues();
    computeLinkYPositions();
    return biHiSankey;
  };

  biHiSankey.link = function () {
    var curvature = defaultLinkCurvature;

    function leftToRightLink(link) {
      var arrowHeadLength = link.thickness * arrowheadScaleFactor,
          straightSectionLength = (3 * link.thickness / 4) - arrowHeadLength,
          x0 = link.source.x + link.source.width,
          x1 = x0 + arrowHeadLength / 2,
          x4 = link.target.x - straightSectionLength - arrowHeadLength,
          xi = d3.interpolateNumber(x0, x4),
          x2 = xi(curvature),
          x3 = xi(1 - curvature),
          y0 = link.source.y + link.sourceY + link.thickness / 2,
          y1 = link.target.y + link.targetY + link.thickness / 2;
      return "M" + x0 + "," + y0
           + "L" + x1 + "," + y0
           + "C" + x2 + "," + y0
           + " " + x3 + "," + y1
           + " " + x4 + "," + y1
           + "L" + (x4 + straightSectionLength) + "," + y1;
    }

    function rightToLeftLink(link) {
      var arrowHeadLength = link.thickness * arrowheadScaleFactor,
          straightSectionLength = link.thickness / 4,
          x0 = link.source.x,
          x1 = x0 - arrowHeadLength / 2,
          x4 = link.target.x + link.target.width + straightSectionLength + arrowHeadLength,
          xi = d3.interpolateNumber(x0, x4),
          x2 = xi(curvature),
          x3 = xi(1 - curvature),
          y0 = link.source.y + link.sourceY + link.thickness / 2,
          y1 = link.target.y + link.targetY + link.thickness / 2;
      return "M" + x0 + "," + y0
           + "L" + x1 + "," + y0
           + "C" + x2 + "," + y0
           + " " + x3 + "," + y1
           + " " + x4 + "," + y1
           + "L" + (x4 - straightSectionLength) + "," + y1;
    }

    function link(d) {
      if (d.source.x < d.target.x) {
        return leftToRightLink(d);
      }
      return rightToLeftLink(d);
    }

    link.curvature = function (_) {
      if (!arguments.length) { return curvature; }
      curvature = +_;
      return link;
    };

    return link;
  };

  biHiSankey.links = function (_) {
    if (!arguments.length) { return links; }
    links = _.filter(function (link) {
      return link.source !== link.target; // filter out links that go nowhere
    });
    return biHiSankey;
  };

  biHiSankey.linkSpacing = function (_) {
    if (!arguments.length) { return linkSpacing; }
    linkSpacing = +_;
    return biHiSankey;
  };

  biHiSankey.nodes = function (_) {
    if (!arguments.length) { return nodes; }
    nodes = _;
    return biHiSankey;
  };

  biHiSankey.nodeWidth = function (_) {
    if (!arguments.length) { return nodeWidth; }
    nodeWidth = +_;
    return biHiSankey;
  };

  biHiSankey.nodeSpacing = function (_) {
    if (!arguments.length) { return nodeSpacing; }
    nodeSpacing = +_;
    return biHiSankey;
  };

  biHiSankey.relayout = function () {
    computeLeftAndRightLinks();
    computeNodeValues();
    computeLinkYPositions();
    return biHiSankey;
  };

  biHiSankey.size = function (_) {
    if (!arguments.length) { return size; }
    size = _;
    return biHiSankey;
  };

  biHiSankey.visibleLinks = function () {
    return visible(links);
  };

  biHiSankey.initializeNodes = function (callback) {
    initializeNodeMap();
    computeNodeHierarchy();
    computeNodeLinks();
    computeAncestorLinks();
    mergeLinks();
    computeConnectedNodes();
    nodes.forEach(callback);
    return biHiSankey;
  };

  return biHiSankey;
};
