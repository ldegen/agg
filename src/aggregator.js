var ATTR_DELIM = '.';
//var MAPPING_PATTERN = /^(((\w+(\[\])?\.)*)|\*\.)\w+(\[\]|[#+])?$/;
var MAPPING_PATTERN = /(^(((\w+(\[\])?[.])*\w+(\[\])?)?:)?(\w+(\[\])?[.])*\w+(\[\]|[#+])?$)|(^\*\.(\w+\.)*\w+[#+]?$)/;

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
  var wildcard = {};
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
    var segments_ = segments.map(function(segment) {
      return segment.match(/\w+|\*/)[0];
    });
    var key = segments_.join('.');
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
          attribute: attribute.match(/\w+|\*/)[0],
          root: false,
          depth: depth,
          leaf: !child,
          multiValued: !!attribute.match(/\[\]$/),
          unique: !!attribute.match(/[#+]$/)
            //      parentType: PartType(segments)
        };
        if (attribute == '*') {
          partType.wildcard = true;
          partType.wildcardRoot = true;
        } else {
          partType.parentType = PartType(segments, partType);
          partType.wildcard = partType.parentType.wildcard;
        }
      }
      partTypes[key] = partType;

    }
    if (child) {
      partType.childrenTypes[child.attribute] = child;
    }
    return partType;
  };

  var processHeaderCell = function(label, colNum) {
    if (!label.match(MAPPING_PATTERN)) {
      throw new Error("malformed column mapping: " + label);
    }
    var m = label.match(/^(?:([^:]*):)?([^:]+)$/);
    var prefixSegments = [];
    var prefixPartType;
    if (typeof m[1] !== "undefined") {
      prefixSegments = m[1] ? m[1].split(/[.]/) : [];
      prefixPartType = PartType(prefixSegments.slice());
    }
    var segments = prefixSegments.concat(m[2].split(/[.]/));
    var partType = PartType(segments.slice());
    partType.prefix = prefixPartType;
    if (partType.used) {
      throw new Error("attribute appears more than once: " + label);
    }
    partType.used = true;
    return partType;
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
    //terminal case: wildcard
    if (partType.wildcardRoot) {
      return wildcard;
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
    if (partType.wildcardRoot) {
      return wildcard;
    }

    var parent;
    if (partType.multiValued) {
      parent = currentPart(partType.parentType);
      var mountPoint = parent[partType.attribute];
      var part = {};
      mountPoint.push(part);
      return part;
    } else {
      var parent = startNewPart(partType.parentType);
      var part = {};
      parent[partType.attribute] = part;
      return part;
    }
  };

  var removeAncestorsFromWildcard = function(partType) {
    var anc = ancestorInWildcard(partType);
    if (typeof anc === "string" &&  anc!==partType.key && !wildcard[anc].keep) {
    //  console.log("wc remove '"+partType.key+"'");
      delete wildcard[anc];
    }
  };

  var addToWildcard = function(part, partType, colType, value) {
    removeAncestorsFromWildcard(partType);
    if (!colType.wildcard) {
    //  console.log("wc add '"+partType.key+"'");
      if (colType.prefix) {
        wildcard[colType.prefix.key] = {
          part: currentPart(colType.prefix),
          partType: colType.prefix,
          keep: true
        };
      } else {
        wildcard[partType.key] = {
          part: part,
          partType: partType,
          keep: false
        };
      }
    }
  };

  var putValue = function(part, partType, colType, value) {
//    console.log("put to '"+partType.key+"'",colType.key,value);
    if (colType.multiValued) {
      if (!part[colType.attribute]) {
        part[colType.attribute] = [];
      }
      part[colType.attribute].push(value);
      removeAncestorsFromWildcard(colType);
    } else {
      // if a second value is encountered for a single-value
      // attribute, there are two cases to examine:
      if (part.hasOwnProperty(colType.attribute)) {
        var prevVal = part[colType.attribute];

        //if the value is the same as the previous, and the
        //attribute is marked "unique", just skip the cell.
        if (prevVal == value && colType.unique) {
          addToWildcard(part,partType,colType,value);
          return;
        }
        //if the value is for a wildcard attribute, we must
        //raise an exception. A conflicting assignment typically means
        //a wrong mapping
        if (colType.wildcard) {
          throw new Error("conflicting assignment for wildcard attribute '" + colType.attribute + "' in part '" + partType.key + "': " + value + "(prev. Value:" + prevVal + ")" + root.id);
        }
        //otherwise create a new part
        else {
          part = startNewPart(partType);
        }
      }
      part[colType.attribute] = value;
      addToWildcard(part, partType, colType, value);
    }
  };
  var ancestorInWildcard = function(partType) {
    if (wildcard.hasOwnProperty(partType.key)) {
      return partType.key;
    }
    if (partType.root || partType.wildcard) {
      return false;
    }
    return ancestorInWildcard(partType.parentType);
  };



  var processCell = function(value, colNum) {
    // empty cells are ignored *completely*.
    if (typeof value == "undefined" || value === null || value === "") {
      return;
    }

    var colType = columnTypes[colNum];
    var partType = colType.parentType;

    var part = currentPart(partType);

    if (part === wildcard) {
      Object.keys(wildcard).forEach(function(key) {
        var target = wildcard[key];
        //console.log("add wc "+colType.key+"="+value+" to '"+target.partType.key+"'");
        putValue(target.part, target.partType, colType, value);
      });
    } else {
      //console.log("add  "+colType.key+"="+value+" to '"+partType.key+"'");
      putValue(part, partType, colType, value);
    }

  };

  var processRow = function(row, rownum) {
    if (columnTypes) {
      //reset wildcard targets for each row!
      wildcard = {};
      columnOrder.map(function(i) {
        try {
          processCell(row[i], i);
        } catch (e) {
          throw new Error(rownum + "," + i + ": " + e.message);
        }
      });
    } else {
      processHeaderRow(row);
    }
  };

  var processHeaderRow = function(headerCells) {
    columnTypes = headerCells.map(processHeaderCell);


    // - concrete comes before wildcard
    // - short paths come before long paths
    // - single-valued come before multi-valued

    columnOrder = columnTypes.map(function(colType, colPos) {
      return {
        k: 2 * colType.depth + (colType.multiValued ? 1 : 0),
        wc: colType.wildcard ? 1 : 0,
        i: colPos
      };
    }).sort(function(a, b) {
      var wc = a.wc - b.wc;
      if (wc !== 0) {
        return wc;
      }
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
  var rownum = 0;

  tf._transform = function(chunk, enc, done) {
    p.row(chunk, rownum++);
    done();
  };
  tf._flush = function(done) {
    p.commit();
  };

  return tf;
};
