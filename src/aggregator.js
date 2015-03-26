module.exports = function() {
  var columnTypes = [];
  var documents = [];
  var ATTR_DELIM = '.';
  var root;
  var partTypes = {};

  var commit = function() {
    if (root) {
      documents.push(root);
      root = null;
    }
  };

  var values = function(obj){
    return Object.keys(obj).map(function(key){
      return obj[key];
    });
  };

  var prop = function(key){
    return function(obj){
      return obj[key];
    };
  };

  var detectAmbiguities = function() {
    values(partTypes).forEach(function(partType){
      if(partType.leaf){
        return;
      }
      if(partType.multiValued){
        if(values(partType.childrenTypes).every(prop("multiValued"))){
          throw new Error("Ambiguous column mapping: the multi-valued part '"
                          + partType.key +
                          "' must have at least one single-valued attribute."
                         );
        }
      }
      if(partType.root){
        if(values(partType.childrenTypes).every(prop("multiValued"))){
          throw new Error("Ambiguous column mapping: documents must have at least one single-valued attribute.");
        }
      }
    });
  };

  var PartType = function(segments, child) {
    var key = segments.join('.');
    var partType = partTypes[key];
    if (!partType) {
      if (segments.length === 0) {
        partType = {
          key:key,
          root: true,
          childrenTypes: {}
        };
      } else {
        var attribute = segments.pop();
        var partType = {
          key:key,
          childrenTypes: {},
          attribute: attribute.match(/[^\[]+/)[0],
          root: false,
          leaf: !child,
          multiValued: !!attribute.match(/\[\]$/),
          //      parentType: PartType(segments)
        };
        partType.parentType = PartType(segments, partType);
      }
      partTypes[key]=partType;
    }
    if (child) {
      partType.childrenTypes[child.attribute] = child;
    }
    return partType;
  };

  var processHeader = function(label) {
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
    if (typeof value == "undefined" || value === null) {
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
      // if a second attribute is encountered for a single-value
      // attribute, create a new part.
      if (part.hasOwnProperty(colType.attribute)) {
        part = startNewPart(colType.parentType);
      }
      part[colType.attribute] = value;
    }
  };

  var processRow = function(row) {
    row.forEach(processCell);
  };

  var processTable = function(rows) {
    columnTypes = rows.shift().map(processHeader);
    detectAmbiguities();
    rows.forEach(processRow);
    commit();
    return documents;
  };

  return {
    processTable: processTable
  };
}
