var ATTR_DELIM = '.';
var MAPPING_PATTERN = /^(\w+(\[\])?\.)*\w+(\[\]|#)?$/;

var stream = require("stream");


var values = function(obj) {
  return Object.keys(obj).map(function(key) {
    return obj[key];
  });
};

var prop = function(key) {
  return function(obj) {
    return obj[key];
  };
};

var Processor = function(push) {

  var stream = require("stream");
  var columnTypes;
  var columnOrder;
  var root;
  var partTypes = {};


  var detectAmbiguities = function() {
    values(partTypes).forEach(function(partType) {
      if (partType.leaf) {
        return;
      }
      if (partType.multiValued) {
        if (values(partType.childrenTypes).every(prop("multiValued"))) {
          throw new Error("Ambiguous column mapping: the multi-valued part '" + partType.key +
            "' must have at least one single-valued attribute."
          );
        }
      }
      if (partType.root) {
        if (values(partType.childrenTypes).every(prop("multiValued"))) {
          throw new Error("Ambiguous column mapping: documents must have at least one single-valued attribute.");
        }
      }
    });
  };

  var PartType = function(segments, child) {
    var key = segments.join('.');
    var depth = segments.length;
    var partType = partTypes[key];
    if (!partType) {
      if (depth === 0) {
        partType = {
          key: key,
          root: true,
          depth: 0,
          childrenTypes: {}
        };
      } else {
        var attribute = segments.pop();
        var partType = {
          key: key,
          childrenTypes: {},
          attribute: attribute.match(/\w+/)[0],
          root: false,
          depth: depth,
          leaf: !child,
          multiValued: !!attribute.match(/\[\]$/),
          unique: !!attribute.match(/#$/)
          //      parentType: PartType(segments)
        };
        partType.parentType = PartType(segments, partType);
      }
      partTypes[key] = partType;

    }
    if (child) {
      partType.childrenTypes[child.attribute] = child;
    }
    return partType;
  };

  var processHeaderCell = function(label) {
    if (!label.match(MAPPING_PATTERN)) {
      throw new Error("malformed column mapping: " + label);
    }
    var segments = label.split(ATTR_DELIM);
    return PartType(segments)
  };

  // *always* returns an object. Never null.
  var currentPart = function(partType) {
    //terminal case: root part type.
    if (partType.root) {
      if (!root) {
        root = {};
      }
      return root;
    }

    //recursive case
    var parent = currentPart(partType.parentType);
    if (!parent[partType.attribute]) {
      parent[partType.attribute] = partType.multiValued ? [{}] : {};
    }
    var mountPoint = parent[partType.attribute];
    return partType.multiValued ? mountPoint[mountPoint.length - 1] : mountPoint;
  };

  var startNewPart = function(partType) {
    if (partType.root) {
      commit();
      return root = {};
    }

    if (!partType.multiValued) {
      throw new Error("cannot start a new part of single-valued part type " + JSON.stringify(partType));
    }

    var parent = currentPart(partType.parentType);
    var mountPoint = parent[partType.attribute];
    var part = {};
    mountPoint.push(part);
    return part;
  };

  var processCell = function(value, colNum) {
    // empty cells are ignored *completely*.
    if (typeof value == "undefined" || value === null || value === "") {
      return;
    }

    var colType = columnTypes[colNum];
    var part = currentPart(colType.parentType);

    if (colType.multiValued) {
      if (!part[colType.attribute]) {
        part[colType.attribute] = [];
      }
      part[colType.attribute].push(value);
    } else {
      // if a second value is encountered for a single-value
      // attribute, there are two cases to examine:
      if (part.hasOwnProperty(colType.attribute)) {

        //if the value is the same as the previous, and the
        //attribute is marked "unique", just skip the cell.
        if(part[colType.attribute]==value && colType.unique){
          return;
        }
        //otherwise create a new part
        else{
          part = startNewPart(colType.parentType);
        }
      }
      part[colType.attribute] = value;
    }
  };

  var processRow = function(row) {
    if (columnTypes) {
      columnOrder.map(function(i) {
        processCell(row[i], i);
      });
    } else {
      processHeaderRow(row);
    }
  };

  var processHeaderRow = function(headerCells) {
    columnTypes = headerCells.map(processHeaderCell);
    columnOrder = columnTypes.map(function(colType, colPos) {
      return {
        k: 2 * colType.depth + (colType.multiValued ? 1 : 0),
        i: colPos
      };
    }).sort(function(a, b) {
      return a.k - b.k;
    }).map(function(ki) {
      return ki.i;
    });
    detectAmbiguities();

  };


  var commit = function() {
    if (root) {
      push(root);
      root = null;
    }
  };

  return {
    row: processRow,
    commit: commit
  };
};

module.exports.transformSync = function(rows) {
  var documents = [];
  var p = Processor(function(doc) {
    documents.push(doc);
  });
  rows.forEach(p.row);
  p.commit();
  return documents;
};

module.exports.transform = function() {
  var tf = new stream.Transform({
    objectMode: true
  });
  var p = Processor(tf.push.bind(tf));

  tf._transform = function(chunk, enc, done) {
    p.row(chunk);
    done();
  };
  tf._flush = function(done) {
    p.commit();
  };

  return tf;
};
