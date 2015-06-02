describe("The Value Parser",function(){

  var ValueParser = require("../src/value-parser");
  var parse;

  beforeEach(function(){
    parse = ValueParser();
  });

  it("gracefully deals with undefined or null values", function(){
    expect(parse()).to.equal(undefined);
  });

  it("returns strings unchanged by default",function(){
    expect(parse("don't touch me, i'm not special")).to.equal("don't touch me, i'm not special");
  });

  it("detects and parses boolean literals",function(){
    expect(parse("true")).to.equal(true);
    expect(parse("false")).to.equal(false);
  });
  it("detects and parses integers", function(){
    expect(parse("42")).to.equal(42);
    expect(parse("-42")).to.equal(-42);
    expect(parse("0")).to.equal(0);
  });
  it("doesn't parse integers with leading zeros",function(){
    expect(parse("042")).to.equal("042");
    expect(parse(" 042")).to.equal(" 042");
    expect(parse(" 42")).to.equal(42);
  });
  it("detects and parses floats",function(){
     expect(parse("0.42")).to.equal(0.42);
  });
});
