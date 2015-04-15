var ATTR_DELIM = '.';
var MAPPING_PATTERN = /^(((\w+(\[\])?\.)*)|\*\.)\w+(\[\]|[#+])?$/;

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
      return segment.match(/\w+|\*/).toString();
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
          partType.wildcard=partType.parentType.wildcard;
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
    var segments = label.split(ATTR_DELIM);
    var partType = PartType(segments);
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
    if (partType.wildcardRoot){
      return wildcard = {};
    }

    if (!partType.multiValued) {
      throw new Error("cannot start a new part of single-valued part type " + partType.key);
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

    var putValue = function(part) {
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
          if (part[colType.attribute] == value && colType.unique) {
            return;
          }
          //otherwise create a new part
          else {
            part = startNewPart(colType.parentType);
          }
        }
        part[colType.attribute] = value;
        // Now, if this part is not an array, and if this is the first
        // real contribution to this part we want to remember this part, 
        // so that we can add wildcard attributes to it if we encounter 
        // some later (they are always processed last!).
        //
        // There are a few cases, though, that we do not count as
        // "real" contributions:
        //
        // - if the attribute is itself a wildcard
        //
        // - if the attribute is multi-valued (checked above)
        //   In this case the part is an array and the actual contribution is
        //   to one of its elements. Since this may or may not be an object, we
        //   cannot add properties to it.
        //
        // - if the current row already contributed to an an ancestor part
        //   of this part.
        //
        // - if the attribute is anotated as 'unique'. 
        //   In this case, we simply cannot know whether the current row is 
        //   actually contributing anythign to this part, but in most cases 
        //   I can think of, it is not.
        //
        //   TODO: We could actually do better than that. We could allow for a 
        //         special column that explicitly communicates the part type a 
        //         row is contributing to.
        if(!colType.unique && !colType.wildcard &&! isInWildcard(colType.parentType)){
          //addToWildcard(part);
          wildcard[colType.parentType.key]=part;
        }
      }
    };
    var isInWildcard = function(partType){
      if(wildcard.hasOwnProperty(partType.key)){
        return true;
      }
      if(partType.root || partType.wildcard){
        return false;
      }
      return isInWildcard(partType.parentType);
    };

    var part = currentPart(colType.parentType);
    if(part === wildcard){
      Object.keys(wildcard).forEach(function(key){
        putValue(wildcard[key]);
      });
    }else{
      putValue(part);
    }

  };

  var processRow = function(row) {
    if (columnTypes) {
      //reset wildcard targets for each row!
      wildcard = {};
      columnOrder.map(function(i) {
        processCell(row[i], i);
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
      if(wc!==0){
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

  tf._transform = function(chunk, enc, done) {
    p.row(chunk);
    done();
  };
  tf._flush = function(done) {
    p.commit();
  };

  return tf;
};
