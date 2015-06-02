module.exports = function(options) {
  return function(s0) {
    if (typeof s0 !== "string") {
      return s0;
    }
    var s = s0.trim();
    if(s===""){
      return s0;
    }
    if (s === 'true') {
      return true;
    }
    if (s == 'false') {
      return false;
    }
    if (/^0\d+$/.test(s)) {
      return s0;
    }
    var num = Number(s);
    if (!isNaN(num)) {
      return num;
    }
    return s0;
  };
};
