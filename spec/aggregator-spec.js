describe("the aggregator", function(){
  var Aggregator = require("../src/aggregator");
  var aggregator;

  beforeEach(function(){
    aggregator=Aggregator();
  });

  it("supports simple (single-token) attributes",function(){
     var docs = aggregator.processTable([
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
     var docs = aggregator.processTable([
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
     var docs = aggregator.processTable([
       [ "value"   , "names[]" ],
       [ 1         , "Eins"    ],
       [ undefined , "Egy"     ],
       [ 2         , "Zwei"    ],
       [ null      , "Kettő"   ],
       [ 3         , "Drei"    ],
       [ undefined , "Három"   ]
     ]);

     expect(docs).to.eql([
       {value:1, names:["Eins","Egy"]},
       {value:2, names:["Zwei","Kettő"]},
       {value:3, names:["Drei","Három"]}
     ]);
  });

  it("supports multi-valued parts",function(){
     var docs = aggregator.processTable([
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
});
