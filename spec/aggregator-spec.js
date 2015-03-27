describe("the aggregator", function(){
  var Aggregator = require("../src/aggregator");
  var transform = Aggregator.transformSync;


  it("supports simple (single-token) attributes",function(){
     var docs = transform([
       [ "value" , "de"   , "hu"    ],
       [ 1       , "Eins" , "Egy"   ],
       [ 2       , "Zwei" , "Kettő" ],
       [ 3       , "Drei" , "Három" ]
     ]);

     expect(docs).to.eql([
       {value:1, de: "Eins", hu: "Egy"},
       {value:2, de: "Zwei", hu: "Kettő"},
       {value:3, de: "Drei", hu: "Három"}
     ]);
  });

  it("supports nested attributes",function(){
     var docs = transform([
       [ "value" , "name.de" , "name.hu" ],
       [ 1       , "Eins"    , "Egy"     ],
       [ 2       , "Zwei"    , "Kettő"   ],
       [ 3       , "Drei"    , "Három"   ]
     ]);

     expect(docs).to.eql([
       {value:1, name:{de: "Eins", hu: "Egy"}},
       {value:2, name:{de: "Zwei", hu: "Kettő"}},
       {value:3, name:{de: "Drei", hu: "Három"}}
     ]);
  });

  it("supports multi-valued leaf attributes",function(){
     var docs = transform([
       [ "value"   , "names[]" ],
       [ 1         , "Eins"    ],
       [ undefined , "Egy"     ],
       [ 2         , "Zwei"    ],
       [ null      , "Kettő"   ],
       [ 3         , "Drei"    ],
       [ ""        , "Három"   ]
     ]);

     expect(docs).to.eql([
       {value:1, names:["Eins","Egy"]},
       {value:2, names:["Zwei","Kettő"]},
       {value:3, names:["Drei","Három"]}
     ]);
  });

  it("supports multi-valued parts",function(){
     var docs = transform([
       [ "value"   , "names[].lang" , "names[].string" ] ,
       [ 1         , "de"           , "Eins"           ] ,
       [ undefined , "hu"           , "Egy"            ] ,
       [ 2         , "de"           , "Zwei"           ] ,
       [ null      , "hu"           , "Kettő"          ] ,
       [ 3         , "de"           , "Drei"           ] ,
       [ undefined , "hu"           , "Három"          ]
     ]);

     expect(docs).to.eql([
       {value:1, names:[{lang:"de",string:"Eins"},{lang:"hu",string:"Egy"}]},
       {value:2, names:[{lang:"de",string:"Zwei"},{lang:"hu",string:"Kettő"}]},
       {value:3, names:[{lang:"de",string:"Drei"},{lang:"hu",string:"Három"}]}
     ]);
  });

  it("ignores consecutive identical values in 'id#'-fields",function(){
    var docs = transform([
       [ "value#"  , "names[]" ],
       [ 1         , "Eins"    ],
       [ 1         , "Egy"     ],
       [ 2         , "Zwei"    ],
       [ 2         , "Kettő"   ],
       [ 3         , "Drei"    ],
       [ 3         , "Három"   ]
     ]);

     expect(docs).to.eql([
       {value:1, names:["Eins","Egy"]},
       {value:2, names:["Zwei","Kettő"]},
       {value:3, names:["Drei","Három"]}
     ]);
  });

  it("raises an exception if the column mapping is ambiguous", function(){
    var table = [
      [ "tablename" , "rows[].columns[]"] ,
      [ "t1"        , "r0c0"            ] ,
      [ null        , "r0c1"            ] ,
      [ null        , "r1c0"            ] ,
      [ null        , "r1c1"            ]
    ];

    var fn = function(){transform(table);};
    expect(fn).to.throw(/'rows\[\]' must have at least one single-valued attribute/);
  });

  it("also detects ambiguities at the top level", function(){
    var table = [
      [ "words[]"] ,
      [ "w00t"   ] ,
      [ "lol"    ] ,
      [ "orly"   ] ,
      [ "tldr"   ]
    ];

    var fn = function(){transform(table);};
    expect(fn).to.throw(/documents must have at least one single-valued attribute/);
  });

  it("resolves 'simple' ambiguities by reordering the columns", function(){
    var table = [
      [ "words[]" , "id" , "foo[].bang[]" , "foo[].id"] ,
      [ "w00t"    , 2    , "bang 1"       , "foo_1"   ] ,
      [ "lol"     , null , "bang 1"       , "foo_2"   ] ,
      [ "orly"    , null , "bang 2"       , null      ] ,
      [ "tldr"    , 3    , null           , null      ]
    ];
    expect(transform(table)).to.eql([{
        id:2,
        foo:[
          {bang:['bang 1'],id:'foo_1'},
          {bang:['bang 1','bang 2'], id: 'foo_2'}
        ],
        words: ['w00t','lol','orly']
      },{
        id:3,
        words:['tldr']
      }
    ]);
  });


  it("checks the lexical well-formedness of the column mappings", function(){
    var test = function(str){
      return function(){
        transform([ [str]]);
      };
    };
    expect(test("jkl-jh")).to.throw(/malformed column mapping/);
    expect(test(".jkl")).to.throw(/malformed column mapping/);
    expect(test("[].jkl")).to.throw(/malformed column mapping/);
    expect(test("jkl.")).to.throw(/malformed column mapping/);
    expect(test("jkl.[]")).to.throw(/malformed column mapping/);
    expect(test("jkl[][]")).to.throw(/malformed column mapping/);
    expect(test("jkl#.bar")).to.throw(/malformed column mapping/);
    expect(test("jkl[]#")).to.throw(/malformed column mapping/);
    expect(test("jkl#[]")).to.throw(/malformed column mapping/);
    expect(test("jkl[3]")).to.throw(/malformed column mapping/);
    expect(test("jkl[")).to.throw(/malformed column mapping/);
    expect(test("jkl]")).to.throw(/malformed column mapping/);
  });
});
