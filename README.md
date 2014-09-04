DH-400JDBC
===========

JDBC Wrapper for the JT400 driver.


Instructions
=============

1) Require the module:

    var jdbc = require('dh-400jdbc');
    
2) Build the config object:

    var config = {
      serverName: 'myserver',
      libraries: 'MYLIB',
      user: 'usernmame',
      password: 'password',
      secure: false,
      initialPoolCount: 10
    };
    
    jdbc.configure(config);
    
3) Initialize the connection:

    jdbc.initialize(config, function (err) {
      if (err) {
        // HANDLE THE ERROR.
      }
    });
    
4) Execute a SQL query:

    jdbc.executeSqlString('SELECT * FROM TABLENAME', function(err, results) {
      if (err) {
        // HANDLE THE ERROR
      }

      // proccess results.
    });

5) Execute a prepared statement query:
  
  Note: parameters is an array of values.

    jdbc.executePreparedStatement(sql, parameters, function(err, results) {
      if (err) {
        // HANDLE THE ERROR
      }

      // proccess results.
    });
    
6) Execute an update prepared statement:

  Note: parameters is an array of values.

    jdbc.executeUpdatePreparedStatement(sql, parameters, function(err, results) {
      if (err) {
        // HANDLE THE ERROR
      }

      // proccess results.
    });

7) Executing a stored procedure:

  Note: the parameters array is an array of stored procudure parameter objects.
  You can create the objects in this format:

    {
      type: <'in' or 'out'>,
      fieldName: <String>,
      dataType: <String from sql types constants property>,
      value: <any type>
    }

  or use the convenience functions:

    var inputParameter = jdbc.createSPInputParameter(value);
    var outputParameter = jdbc.createSPOutputParameter(sqlDataType, fieldName);

  execute the statement:

    jdbc.executeStoredProcedure(sql, parameters, function(err, result) {
      if (err) {
          // HANDLE THE ERROR
        }

        // proccess results.
    });

  Note: The result object is a key value object where the keys are the output parameter field names.

  {
    <field name 1> : <output param value 1>,
    <field name 2> : <output param value 2>,
    <field name 3> : <output param value 3>,
  }
    
    