#!/usr/bin/env node

var TransformToBulk = require('elasticsearch-streams').TransformToBulk;
var Parse = require('csv-parse');
var Aggregate = require('../src/aggregator');
var Transform = require('stream').Transform;
var Writable = require('stream').Writable;


var argv = require('minimist')(process.argv.slice(2));

var meta = function(doc) {
  return {
    _type: argv.type || 'project',
    _id: doc[argv.id || 'id']
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

var collect = (function() {
  var tf = new Transform({
    objectMode: true
  });
  var document = {}; 
  var idAttr = argv.k;
  var valueAttr = argv.v;
  tf._transform=function(chunk, enc, done) {
    document[chunk[idAttr]]=valueAttr ? chunk[valueAttr] : chunk;
    done();
  };

  tf._flush=function(done){
    this.push(document);
    done();
  };

  return tf;
})();


if (argv.k) {
  process.stdin
    .pipe(parse)
    .pipe(parseBooleans)
    .pipe(aggregate)
    .pipe(collect)
    .pipe(stringify)
    .pipe(process.stdout)
} else {
  process.stdin
    .pipe(parse)
    .pipe(parseBooleans)
    .pipe(aggregate)
    .pipe(toBulk)
    .pipe(stringify)
    .pipe(process.stdout);
}
