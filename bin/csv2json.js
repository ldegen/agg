#!/usr/bin/env node

var TransformToBulk = require('elasticsearch-streams').TransformToBulk;
var WritableBulk = require('elasticsearch-streams').WritableBulk;
var EsClient = require('elasticsearch').Client;
var Parse = require('csv-parse');
var Aggregate = require('../src/aggregator');
var Transform = require('stream').Transform;
var Writable = require('stream').Writable;
var Path = require('path');


var argv = require('minimist')(process.argv.slice(2));

var settings = {
  idAttr: argv.k || 'id',
  valueAttr: argv.v,
  transformPath: argv.T,
  lookupPath: argv.L,
  createEsBulk: argv.b || !!argv.I,
  singleObject: argv.s,
  esType: argv.t || 'project',
  esIndex: argv.I,
  esHost: argv.h || 'http://localhost:9200'
};

var meta = function(doc) {
  return {
    _type: settings.esType,
    _id: doc[settings.idAttr]
  };
};

var parse = Parse({
  auto_parse: true
});
var aggregate = Aggregate.transform();
var toBulk = new TransformToBulk(meta);

var parseBooleans = new Transform({
  objectMode: true
});
parseBooleans._transform = function(chunk, enc, done) {
  this.push(chunk.map(function(v) {
    if (v === 'true') {
      return true;
    } else if (v == 'false') {
      return false;
    }
    return v;
  }));
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

var Transformer = function(transformerPath, lookupPath) {
  var CustomTransformer = require(Path.resolve(transformerPath));
  return lookupPath ? CustomTransformer(require(Path.resolve(lookupPath))) : CustomTransformer();
};

var pipeline = process.stdin
  .pipe(parse)
  .pipe(parseBooleans)
  .pipe(aggregate);

if (settings.singleObject) { //reduce to single object
  pipeline = pipeline.pipe(Collect(settings.idAttr, settings.valueAttr));
}
if (settings.transformPath) { //apply custom transformation
  pipeline = pipeline.pipe(Transformer(settings.transformPath, settings.lookupPath));
}
if (settings.createEsBulk) { //create elasticsearch bulk stream
  pipeline = pipeline.pipe(toBulk);
}
if (settings.esIndex) { //upload to elasticsearch

  var client = new EsClient({
    host: settings.esHost,
    keepAlive: false //wouldn't make sense in our case
  });
  var bulkExec = function(bulkCmds, callback) {
    client.bulk({
      index: settings.esIndex,
      type: settings.esType,
      body: bulkCmds
    }, callback);
  };
  var ws = new WritableBulk(bulkExec);
  ws.on('close', function() {
    client.close();
  });
  pipeline
    .pipe(ws);
} else { //write to stdout
  pipeline
    .pipe(stringify)
    .pipe(process.stdout);
}
