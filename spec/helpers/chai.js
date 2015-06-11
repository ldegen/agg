var chai = require('chai');

var asPromised = require('chai-as-promised');

chai.config.includeStack = true;
chai.use(asPromised);

global.expect = chai.expect;
global.AssertionError = chai.AssertionError;
global.Assertion = chai.Assertion;
global.assert = chai.assert;
