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


//======================================================================================
// SQL Utilities.
//======================================================================================

/**
 * Returns an insert sql statement.
 * @param tableName - The table name.
 * @param parameters - The parameters object.
 * @return {string}
 */
exports.buildInsertStatement = (tableName, parameters) => {
  // start building the insert statement.
  let sql = `INSERT INTO ${tableName}`;

  // check that there are parameters.
  if (Object.keys(parameters).length > 0) {
    // build the parameter lists.
    let columnNamesString = '';
    let valuePlaceHolders = '';

    // keep track of the current index.
    let index = 0;

    // loop over all the keys.
    for (let key in parameters) {
      columnNamesString += key;
      valuePlaceHolders += '?';

      // increment the index.
      index++;

      // if its not the last parameter, add a comma.
      if (index !== Object.keys(parameters).length) {
        columnNamesString += ', ';
        valuePlaceHolders += ', ';
      }
    }

    // connect it to the initial statement.
    sql += ' (' + columnNamesString + ') VALUES (' + valuePlaceHolders + ')';
  }

  return sql;
};

/**
 * Converts a JS object to value array.
 * @param obj - The object.
 */
exports.objectToValueArray = (obj) => {
  let results = [];

  for (let key in obj) {
    if (obj.hasOwnProperty(key)) {
      results.push(obj[key]);
    }
  }

  return results;
};
