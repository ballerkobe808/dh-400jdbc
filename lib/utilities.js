'use strict';

//======================================================================================
// AS400 Parsing Utilities.
//======================================================================================

/**
 * Trims white spade and line endings from AS400 values.
 * @param str - The value.
 */
exports.trimValue = (str) => {
  // default result to null.
  let result = null;

  // if the value is set.
  if (str) {
    result = str.replace(/^\s\s*/, '').replace(/\s\s*$/, '');
  }

  // return the result.
  return result;
};
