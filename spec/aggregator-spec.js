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

  it("supports maps",function(){
     var docs = transform([
       [ "value"   , "names[].lang#" , "names[].string" ] ,
       [ 1         , "de"            , "Eins"           ] ,
       [ undefined , "hu"            , "Egy"            ] ,
       [ 2         , "de"            , "Zwei"           ] ,
       [ null      , "hu"            , "Kettő"          ] ,
       [ 3         , "de"            , "Drei"           ] ,
       [ undefined , "hu"            , "Három"          ]
     ]);

     expect(docs).to.eql([
       {value:1, names:{de: {lang:"de",string:"Eins"}, hu:{lang:"hu",string:"Egy"}}},
       {value:2, names:{de: {lang:"de",string:"Zwei"}, hu:{lang:"hu",string:"Kettő"}}},
       {value:3, names:{de: {lang:"de",string:"Drei"}, hu:{lang:"hu",string:"Három"}}}
     ]);
  });
  it("supports inlining maps", function(){
     var docs = transform([
       [ "value"   , "names[].lang#string" , "names[].string" ],
       [ 1         , "de"                  , "Eins"           ],
       [ undefined , "hu"                  , "Egy"            ],
       [ 2         , "de"                  , "Zwei"           ],
       [ null      , "hu"                  , "Kettő"          ],
       [ 3         , "de"                  , "Drei"           ],
       [ undefined , "hu"                  , "Három"          ]
     ]);

     expect(docs).to.eql([
       {value:1, names:{de: "Eins", hu:"Egy"}},
       {value:2, names:{de: "Zwei", hu:"Kettő"}},
       {value:3, names:{de: "Drei", hu:"Három"}}
     ]);
  });

  it("detects conflicting pk attributes in multi-valued parts",function(){
    var fn = function(table){
      return function(){
        transform(table);
      };
    };
    expect(fn([
        ["id#" , "names[].id#" , "names[].key#" ],
        [ 0    , 1             , ''             ],
        [ 0    , ''            , 1              ]
    ])).to.throw(/2,2: Second pk attribute for part 'names': 'names.key', previous: 'id'/);
    expect(fn([
        ["id#" , "names[].id#" , "*.key#" ],
        [ 0    , 1             , ''       ],
        [ 0    , 2             , 1        ]
    ])).to.throw(/2,2: Second pk attribute for part 'names': '\*\.key', previous: 'id'/);
  });


  it("ignores consecutive identical values in key attribute (e.g. 'id#')",function(){
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

  it("ignores consecutive identical values for unique fields (e.g.'docType+')",function(){
    var docs = transform([
       [ "value+"  , "names[]" ],
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

  describe("conflicting requirements in border case", function(){

    xit("starts a new part if the value of a unique attribute changes from null to non-null", function(){
      var table = [
        [ "value+"  , "names[]" ],
        [ null      , "Nichts"  ],
        [ null      , "Semmi"   ],
        [ 0         , "Null"    ],
        [ null      , "Zéró"    ],
        [ 1         , "Eins"    ],
        [ null      , "Egy"     ],
      ];

      var docs = transform(table);
      console.log("docs", JSON.stringify(docs));

      expect(docs).to.eql([
        {          names: ["Nichts", "Semmi"]},
        {value: 0, names: ["Null","Zéró"]},
        {value: 1, names: ["Eins", "Egy"]}
      ]);
    });

    it("does not require the value of a unique attribute to be passed in the first row of a part", function(){
      var table = [
        [ "value+"  , "names[]" ],
        [ null      , "Null"    ],
        [ 0         , "Zéró"    ],
        [ 1         , "Eins"    ],
        [ null      , "Egy"     ],
      ];

      var docs = transform(table);

      expect(docs).to.eql([
        {value: 0, names: ["Null","Zéró"]},
        {value: 1, names: ["Eins", "Egy"]}
      ]);
    });
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
    expect(fn).to.throw(/'rows' must have at least one single-valued attribute/);
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


  it("can demux wildcards to currently 'relevant' parts",function(){
    var table =[
      [ "*.ort"   , "event"       , "persons[].name" , "location.name" ],
      [ ""        , "Pustekuchen" , ""               , ""              ],
      [ "Berlin"  , ""            , ""               , "bcc"           ],
      [ "Bonn"    , ""            , "Paul"           , ""              ],
      [ "Leibzig" , ""            , "Peter"          , ""              ]
    ];
    expect(transform(table)).to.eql([{
      event: 'Pustekuchen',
      location:{
        name: 'bcc',
        ort: 'Berlin'
      },
      persons:[{
        name:'Paul',
        ort:'Bonn'
      },{
        name:'Peter',
        ort:'Leibzig'
      }]
    }]);
  });

  it("only adds wildcard attrs to leaf parts by default",function(){
    var table = [
      ["*.row" , "id+" , "persons[].id" , "persons[].role" , "title.de"        , "title.en"      ],
      [1       , 100   , 101            , 'foo'            , ''                , ''              ],
      [2       , 100   , 102            , 'foo'            , ''                , ''              ],
      [3       , 100   , ''             , ''               , 'Deutscher Titel' , 'English Title' ]
    ];

    expect(transform(table)).to.eql([{
      id:100,
      persons:[{
        row:1,
        id:101,
        role:'foo'
      },{
        row:2,
        id:102,
        role:'foo'
      }],
      title:{
        row:3,
        de:'Deutscher Titel',
        en:'English Title'
      }
    }]);
  });
  it("ignores wildcards if all contributed leafs are multivalued", function(){
    //in particular, it does NOT try to add the wc attrs to ancestor parts
    var table = [
      ["id+" , "*.wc" , "multi[]" ],
      [ 10   , 1      , 'a'       ],
      [ 10   , 2      , 'b'       ],
      [ 10   , 3      , ''        ],
    ];
    expect(transform(table)).to.eql([{
      id:10,
      multi:['a','b']
    }]);
  });
  it("delegates wildcards up to parent if a part is written in parenthesis",function(){
    var table = [
      ["*.row" , "id+" , ":title.kurz.de"   , "title:lang.en"      ],
      [1       , 100   , 'Deutscher Titel' , ''                    ],
      [2       , 100   , ''                , 'English Title'       ]
    ];
    expect(transform(table)).to.eql([{
      id:100,
      row:1,
      title:{
        kurz:{
          de:'Deutscher Titel'
        },
        lang:{
          en:'English Title'
        },
        row: 2
      }
    }]);
  });
  it("supports adding wildcards to multivalued arguments",function(){
    var table = [
      ['id+' , '*.foo' , 'multi[]:bar' ],
      [100   , 10      , 1             ],
      [100   , 20      , 2             ],
      [100   , 30      , 3             ]
    ];
    expect(transform(table)).to.eql([{
      id:100,
      multi:[{
        foo:10,
        bar:1
      },{
        foo:20,
        bar:2
      },{
        foo:30,
        bar:3
      }]
    }]);

  });

  it("supports adding wildcards to multivalued arguments",function(){
    var table = [
      ['id+' , '*.foo' , 'multi[]:bar.baz' ],
      [100   , 10      , 1             ],
      [100   , 20      , 2             ],
      [100   , 30      , 3             ]
    ];
    expect(transform(table)).to.eql([{
      id:100,
      multi:[{
        foo:10,
        bar:{baz:1}
      },{
        foo:20,
        bar:{baz:2}
      },{
        foo:30,
        bar:{baz:3}
      }]
    }]);
  });

  it("throws an error if conflicting wildcard attr values are assigned to the same part",function(){
    var table = [
      ["*.row" , "id+" , "title.de"        , "title.en"      ],
      [2       , 100   , ''                , 'English Title' ],
      [3       , 100   , 'Deutscher Titel' , ''              ]
    ];
    var fn = function(){transform(table);};
    expect(fn).to.throw(/conflicting assignment for wildcard attribute 'row'/);
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

  it("raises an exception if more than one column is mapped on the same attribute",function(){
    var test = function(){
      var labels = Array.prototype.slice.call(arguments);
      return function(){
        transform([ labels]);
      };
    };

    expect(test("foo","foo")).to.throw(/attribute appears more than once.+foo/);
    expect(test("foo+","foo+")).to.throw(/attribute appears more than once.+foo/);
    expect(test("foo#","foo#")).to.throw(/attribute appears more than once.+foo/);
    expect(test("foo+","foo#")).to.throw(/attribute appears more than once.+foo/);
    expect(test("foo#","foo+")).to.throw(/attribute appears more than once.+foo/);
    expect(test("foo+","foo")).to.throw(/attribute appears more than once.+foo/);
    expect(test("foo#","foo")).to.throw(/attribute appears more than once.+foo/);
    expect(test("foo","foo+")).to.throw(/attribute appears more than once.+foo/);
    expect(test("foo","foo#")).to.throw(/attribute appears more than once.+foo/);

    expect(test("foo[]","foo+")).to.throw(/attribute appears more than once.+foo/);
    expect(test("foo.bar","foo.bar#")).to.throw(/attribute appears more than once.+foo/);
  });
});
