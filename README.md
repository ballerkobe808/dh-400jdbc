DH-400JDBC
===========

JDBC Wrapper for the JT400 driver.


Instructions
=============

1) Require the module:

    var jdbc = require('dh-400jdbc')();
    
2) Build the config object:

    var config = {
      serverName: 'myserver',
      libraries: 'MYLIB',
      user: 'usernmame',
      password: 'password',
      secure: false,
      initialPoolCount: 10
    };
    
3) Initialize the connection:

    jdbc.initialize(config, function (err) {
      if (err) {
        // HANDLE THE ERROR.
      }
    });
    
4) Execute a SQL query:

    jdbc.executeSql('SELECT * FROM TABLENAME', function(err, results) {
      if (err) {
        // HANDLE THE ERROR
      }
      
      // HANDLE RESULTS
      // results is an array of the returned rows.
    });

5) Execute a prepared statement query:

    jdbc.prepareStatementWithParameterArray(sql, parameters, function (err, results) {
      if (err) {
        // HANDLE THE ERROR
      }
        
      // HANDLE RESULTS
      // results is an array of the returned rows.
    });
    
6) Execute an update prepared statement:

    jdbc.prepareStatementUpdateWithParameterArray(sql, parameters, function (err) {
      if (err) {
        // HANDLE THE ERROR
      }
    });
    
    