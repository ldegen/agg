#!/usr/bin/env node
require("coffee-script/register");
var Aggregate = require('../src/aggregator');
var Cli = require('../src/cli');
var cli = Cli(process);

cli.input()
.pipe(cli.preprocessor())
.pipe(Aggregate.transform(cli.errorHook))
.pipe(cli.postprocessor())
.pipe(cli.output());

