#!/usr/bin/env node

var Aggregate = require('../src/aggregator');
var Cli = require('../src/cli');
var cli = Cli(process);

cli.input()
.pipe(cli.preprocessor())
.pipe(Aggregate.transform(cli.errorHook))
.pipe(cli.postprocessor())
.pipe(cli.output());

