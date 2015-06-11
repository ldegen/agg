module.exports = function(name) {
  return function(lookup) {
    Transform = require('stream').Transform;
    transform = new Transform({
      objectMode: true
    });
    transform._transform = function(chunk, enc, done) {
      this.push({
        name: name,
        lookup: lookup,
        chunk: chunk
      });
      return done();
    };
    return transform;
  };
};
