var TransformToBulk = require('elasticsearch-streams').TransformToBulk;
var WritableBulk = require('elasticsearch-streams').WritableBulk;
var Path = require('path');
var EsClient = require('elasticsearch').Client;
var Parse = require('csv-parse');
var Transform = require('stream').Transform;
var combine = require('stream-combiner');
var fs = require("fs");
module.exports = function(process) {
  var argv = require('minimist')(process.argv.slice(2));
  var errorHooks = [];

  var settings = {
    idAttr: argv.k || 'id',
    typeAttr: argv.y || 'type',
    valueAttr: argv.v,
    transformPath: argv.T,
    lookupPath: argv.L,
    filterPath: argv.F,
    createEsBulk: argv.b || !!argv.I || !!argv.S,
    singleObject: argv.s || !!argv.S,
    fixedDocumentId: argv.S,
    esType: argv.t || 'project',
    esIndex: argv.I,
    esHost: argv.h || (process.env || {}).ES_URL || 'http://localhost:9200',
    esBulkTimeout: argv["es-bulk-timeout"] || 120000,
    esBulkChunkSize: argv["es-bulk-chunk-size"] || 128,
    csvDelimiter: argv.D, /* no default, the parser should fall back to comma. I think. */
    csvEncoding: argv.E, /* no default, should use platform default */
    csvRecordDelimiter: argv.R, /* none by default --> auto detect */
  };

  var meta = function(doc) {
    return {
      _type: doc[settings.typeAttr] || settings.esType,
      _id: settings.fixedDocumentId || doc[settings.idAttr]
    };
  };

  var toBulk = new TransformToBulk(meta);

  var parse = Parse({
    auto_parse: false,
    delimiter: settings.csvDelimiter,
    record_delimiter: settings.csvRecordDelimiter
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
      var customTransform = lookupPath ? Factory(lookup) : Factory();
      if (typeof customTransform.errorHook =="function"){
        console.error("register errorHook");
        errorHooks.push(customTransform.errorHook.bind(customTransform));
      }
      return customTransform;
    });
  };
  var createEsSink = function(host, index) {
    var client = new EsClient({
      host: host,
      keepAlive: false, //wouldn't make sense in our case
      requestTimeout: settings.esBulkTimeout
    });
    var bulkExec = function(bulkCmds, callback) {
      client.bulk({
        index: index,
        body: bulkCmds
      }, callback);
    };
    var ws = new WritableBulk(bulkExec, settings.esBulkChunkSize /* high-water-mark a.k.a. chunk size. */);
    ws.on('close', function() {
      client.close();
    });
    return ws;
  };
  return {
    input: function() {
      var stream;
      if(argv._.length>0){
        stream= fs.createReadStream(argv._[0]);
      } else {
        stream = process.stdin; 
      }
      if(settings.csvEncoding){
        //FIXME: for some reason, this does not work?
        //stream.setEncoding(settings.csvEncoding);
        //
        //So we go the long way:
        tf = new Transform({
          transform: function(chunk, enc, done){
            this.push(chunk.toString(settings.csvEncoding));
            done();
          }
        });
        stream = stream.pipe(tf);
      }
      return stream.pipe(parse).pipe(parseValues);
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
    },
    errorHook: function(error, row, col){
      errorHooks.forEach(function(hook){
        hook.call(null,error,row,col);
      });
    }
  };
};
