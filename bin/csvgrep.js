var Transform = require('stream').Transform;
var Writable = require('stream').Writable;
var argv = require('minimist')(process.argv.slice(2));
var Parse = require('csv-parse');
var Stringify = require('csv-stringify');
var parse = Parse({
  auto_parse: false
});
var stringify = Stringify();
var settings = {
  rowSpec: argv.r,
  colSpec: argv.c
};


var processHeader = function(cells){
  
};

var processRow = function(cells, rowNum){

};


var transform = new Transform({
  objectMode: true
});
transform._transform = function(chunk, enc, done) {
  if (rowNum++ == 0) {
    this.push(processHeader(chunk));
  } else {
    var row = processRow(chunk, rowNum);
    if (row) {
      this.push(row);
    }
  }
  done();
};

var pipeline = process.stdin
  .pipe(parse)
  .pipe(transform)
  .pipe(stringify)
  .pipe(process.stdout);
