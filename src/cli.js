var TransformToBulk = require('elasticsearch-streams').TransformToBulk;
var WritableBulk = require('elasticsearch-streams').WritableBulk;
var Path = require('path');
var EsClient = require('elasticsearch').Client;
var Parse = require('csv-parse');
var Transform = require('stream').Transform;
var combine = require('stream-combiner');
module.exports = function(process) {
  var argv = require('minimist')(process.argv.slice(2));

  var settings = {
    idAttr: argv.k || 'id',
    typeAttr: argv.y || 'type',
    valueAttr: argv.v,
    transformPath: argv.T,
    lookupPath: argv.L,
    filterPath: argv.F,
    createEsBulk: argv.b || !!argv.I,
    singleObject: argv.s,
    esType: argv.t || 'project',
    esIndex: argv.I,
    esHost: argv.h || 'http://localhost:9200'
  };

  var meta = function(doc) {
    return {
      _type: doc[settings.typeAttr] || settings.esType,
      _id: doc[settings.idAttr]
    };
  };

  var toBulk = new TransformToBulk(meta);

  var parse = Parse({
    auto_parse: false
  });
  var parseValues = new Transform({
    objectMode: true
  });

  var parseValue = require("../src/value-parser.js")();
  parseValues._transform = function(chunk, enc, done) {
    this.push(chunk.map(parseValue));
    done();
  };

  var stringify = new Transform({
    objectMode: true
  });
  stringify._transform = function(chunk, enc, done) {
    this.push(JSON.stringify(chunk) + "\n");
    done();
  };
  var Collect = function(idAttr, valueAttr) {
    var tf = new Transform({
      objectMode: true
    });
    var document = {};
    tf._transform = function(chunk, enc, done) {
      document[chunk[idAttr]] = valueAttr ? chunk[valueAttr] : chunk;
      done();
    };

    tf._flush = function(done) {
      this.push(document);
      done();
    };

    return tf;
  };

  var loadCustomTransorms = function(modules, lookupPath) {
    if (typeof modules == "string") {
      modules = [modules];
    }
    var lookup = lookupPath ? require(Path.resolve(lookupPath)) : null;
    return modules.map(function(module) {
      var Factory = require(Path.resolve(module));
      return lookupPath ? Factory(lookup) : Factory();
    });
  };
  var createEsSink = function(host, index) {
    var client = new EsClient({
      host: host,
      keepAlive: false //wouldn't make sense in our case
    });
    var bulkExec = function(bulkCmds, callback) {
      client.bulk({
        index: index,
        body: bulkCmds
      }, callback);
    };
    var ws = new WritableBulk(bulkExec);
    ws.on('close', function() {
      client.close();
    });
    return ws;
  };
  return {
    input: function() {
      return process.stdin.pipe(parse).pipe(parseValues);
    },
    preprocessor: function() {
      var ps = settings.filterPath ? loadCustomTransorms(settings.filterPath, settings.lookupPath) : [];
      return combine(ps);
    },
    postprocessor: function() {
      var ps = settings.transformPath ? loadCustomTransorms(settings.transformPath, settings.lookupPath) : [];
      if (settings.singleObject) {
        ps.push(Collect(settings.idAttr, settings.valueAttr));
      }
      if (settings.createEsBulk) {
        ps.push(toBulk);
      }
      return combine(ps);
    },
    output: function() {
      if (settings.esIndex) {
        return createEsSink(settings.esHost, settings.esIndex);
      } else {
        stringify.pipe(process.stdout);
        return stringify;
      }
    }
  };
};
