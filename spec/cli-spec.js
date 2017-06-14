describe("The Command Line Interface (CLI)", function() {
  var Promise = require("promise");
  var Readable = (require("stream")).Readable;
  var Writable = (require("stream")).Writable;
  var Path = require("path");
  var Cli = require("../src/cli");
  var concat = require("concat-stream");
  var through = require("through");
  var proc;

  var Output = function(opts0) {
    var opts = opts0 || {};
    var buf = opts.objectMode ? [] : new Buffer([]);
    var output = new Writable(opts);
    output._write = function(chunk, enc, next) {
      if (chunk) {
        if (opts.objectMode) {
          buf.push(chunk);
        } else {
          buf = Buffer.concat([buf, chunk]);
        }
      }
      next();
    };
    output.promise = new Promise(function(resolve, reject) {
      output.on("error", reject);
      output.on("finish", function() {
        resolve(opts.objectMode ? buf : buf.toString().trim());
      });
    });
    return output;
  };

  var read = function(readable) {
    var buf = [];
    return new Promise(function(resolve, reject) {
      readable.on("error", reject);
      readable.on("readable", function() {
        var r = readable.read();
        if (r) {
          buf.push(r);
        }
      });
      readable.on("end", function() {
        resolve(buf);
      });
    });
  };
  var input = function(s) {
    proc.stdin.push(s);
    proc.stdin.push(null);
  };


  var output = function() {
    return proc.stdout.promise;
  }
  beforeEach(function() {
    return proc = {
      stdin: new Readable(),
      stdout: Output(),
      argv: ['node', 'cli.js']
    };
  });

  var jsonLines = function(docs) {
    return docs.map(function(doc) {
      return JSON.stringify(doc);
    }).join("\n");
  };
  describe("by default", function() {

    it("parses CSV data from STDIN", function() {
      input("a,b,c\n1,2,3\n");
      return expect(read(Cli(proc).input())).to.eventually.eql([
        ['a', 'b', 'c'],
        [1, 2, 3]
      ]);
    });

    it("writes JSON data to STDOUT", function() {
      var cliout = Cli(proc).output();
      cliout.write({
        foo: "bar"
      });
      cliout.write({
        foo: "knarz"
      });
      cliout.end();
      return expect(output()).to.eventually.eql(jsonLines([{
        foo: "bar"
      }, {
        foo: "knarz"
      }]));
    });

    it("does no preprocessing", function() {
      var pre = Cli(proc).preprocessor();
      var out = pre.pipe(new Output({
        objectMode: true
      }));
      pre.write(42);
      pre.end();
      return expect(out.promise).to.eventually.eql([42]);
    });

    it("does no postprocessing", function() {
      var post = Cli(proc).postprocessor();
      var out = post.pipe(new Output({
        objectMode: true
      }));
      post.write(42);
      post.end();
      return expect(out.promise).to.eventually.eql([42]);
    });
  });
  describe("when given an input file", function(){
    it("reads from that file instead of stdin", function(){
      
      var file = Path.resolve(__dirname, "mock-input.csv");
      proc.argv.push(file);
      return expect(read(Cli(proc).input())).to.eventually.eql([
        ['bla', 'blub', 'barf'],
        [4, 2, 42]
      ]);
    });

    it("writes JSON data to STDOUT", function() {
      var cliout = Cli(proc).output();
      cliout.write({
        foo: "bar"
      });


    });
  });
  describe("in ES-Bulk mode", function() {
    it("produces output in ES-Bulk-Index format", function() {
      proc.argv.push('-b');
      var ps = Cli(proc).postprocessor();
      var out = ps.pipe(new Output({
        objectMode: true
      }));

      ps.write({
        id: 42,
        foo: "bar"
      });
      ps.end();
      return expect(out.promise).to.eventually.eql([{
        index: {
          _type: 'project',
          _id: 42
        }
      }, {
        id: 42,
        foo: "bar"
      }]);

    });
    xit("can upload documents to a local ES node", function() {
      //Testing this is probably not worth the trouble?
    });
    xit("can upload documents to a remote ES node", function() {
      //Testing this is probably not worth the trouble?
    });
  });
  describe("in Single Document ES-Bulk mode", function(){
    it("behaves like -b -s, but allows using a fixed document pk ", function(){
      proc.argv.push("--S=-42" /*implies "-b"*/);
      var ps = Cli(proc).postprocessor();
      var out = ps.pipe(new Output({
        objectMode: true
      }));
      ps.write({
        id: 1,
        foo: "bar"
      });
      ps.write({
        id: 2,
        foo: "knarz"
      });
      ps.end();
      return expect(out.promise).to.eventually.eql([{
        index: {
          _type: 'project',
          _id: -42
        }
      }, {
       "1": {
          id: 1,
          foo: "bar"
        },
        "2": {
          id: 2,
          foo: "knarz"
        }
      }]);
    });
  });

  describe("in single-object mode", function() {
    it("reduces output to a single object", function() {
      proc.argv.push("-s");
      var ps = Cli(proc).postprocessor();
      var out = ps.pipe(new Output({
        objectMode: true
      }));
      ps.write({
        id: 1,
        foo: "bar"
      });
      ps.write({
        id: 2,
        foo: "knarz"
      });
      ps.end();
      return expect(out.promise).to.eventually.eql([{
        "1": {
          id: 1,
          foo: "bar"
        },
        "2": {
          id: 2,
          foo: "knarz"
        }
      }]);
    });

    it("allows specifying a custom key attribute", function() {
      proc.argv.push("-s", "-k", "foo");
      var ps = Cli(proc).postprocessor();
      var out = ps.pipe(new Output({
        objectMode: true
      }));
      ps.write({
        id: 1,
        foo: "bar"
      });
      ps.write({
        id: 2,
        foo: "knarz"
      });
      ps.end();
      return expect(out.promise).to.eventually.eql([{
        "bar": {
          id: 1,
          foo: "bar"
        },
        "knarz": {
          id: 2,
          foo: "knarz"
        }
      }]);
    });
    it("allows 'inlining' value attributes", function() {
      proc.argv.push("-s", "-v", "foo");
      var ps = Cli(proc).postprocessor();
      var out = ps.pipe(new Output({
        objectMode: true
      }));
      ps.write({
        id: 1,
        foo: "bar"
      });
      ps.write({
        id: 2,
        foo: "knarz"
      });
      ps.end();
      return expect(out.promise).to.eventually.eql([{
        "1": "bar",
        "2": "knarz"
      }]);
    });
  });

  describe("custom transformations", function() {

    var PATH_TO_A = Path.resolve(__dirname, "mock-transform-a.js");
    var PATH_TO_B = Path.resolve(__dirname, "mock-transform-b.js");
    var PATH_TO_OPTIONS = Path.resolve(__dirname, "mock-options.json");

    it("supports custom preprocessor steps", function() {
      proc.argv.push('-F', PATH_TO_A);
      var pre = Cli(proc).preprocessor();
      var out = pre.pipe(new Output({
        objectMode: true
      }));
      pre.write(42);
      pre.end();
      return expect(out.promise).to.eventually.eql([{
        name: 'a',
        lookup: undefined,
        chunk: 42
      }]);
    });

    it("can load an options module and pass it as argument to the transform factories", function() {
      proc.argv.push('-F', PATH_TO_A, '-L', PATH_TO_OPTIONS);
      var pre = Cli(proc).preprocessor();
      var out = pre.pipe(new Output({
        objectMode: true
      }));
      pre.write(42);
      pre.end();
      return expect(out.promise).to.eventually.eql([{
        name: 'a',
        lookup: {
          foo: "bar"
        },
        chunk: 42
      }]);
    });
    it("can run several transformations in series", function() {
      proc.argv.push('-F', PATH_TO_A, '-F', PATH_TO_B);
      var pre = Cli(proc).preprocessor();
      var out = pre.pipe(new Output({
        objectMode: true
      }));
      pre.write(42);
      pre.end();
      return expect(out.promise).to.eventually.eql([{
        name: 'b',
        lookup: undefined,
        chunk: {
          name: 'a',
          lookup: undefined,
          chunk: 42
        }
      }]);
    });
    it("applies custom postprocessors first (i.e. before applying the builtin ones like -s or -b", function() {
      proc.argv.push('-T', PATH_TO_A, '-b', '-k','name');
      var post = Cli(proc).postprocessor();
      var out = post.pipe(new Output({
        objectMode: true
      }));
      post.write({
        id: 42
      });
      post.end();
      return expect(out.promise).to.eventually.eql([{
        index: {
          _type: 'project',
          _id: 'a'
        }
      }, {
        name: 'a',
        lookup: undefined,
        chunk: {
          id: 42
        }
      }]);
    });
  });

});
