DH-400JDBC
===========

JDBC Wrapper for the JT400 driver to connect to an AS400 using JDBC.


Usage
=============

1. Require the module:

    ```
    const jdbc = require('dh-400jdbc');
    ``` 

2. Initialize the connection:
    ```
    // build the config.
    let config = {
      host: 'String',
      libraries: <String>,
      username: <String>,
      password: <String>,
      initialPoolCount: <Number>,
      logger: <Logger Reference>
    };
   
    // initialize the module.
    jdbc.initialize(config, (err) => {
      // Do Something.
    });
    ```
   
3. Execute a SQL query:
    ```
    jdbc.executeSqlString('SELECT * FROM TABLENAME', (err, results) => {
      // Do Something.
    });
    ````
   
4. Execute a prepared statement query. Note: parameters is an array of values:
    ```
    jdbc.executePreparedStatement(sql, parameters, (err, results) => {
      // Do Something.
    });
    ```
   
5. Execute an update prepared statement. Note: parameters is an array of values:
    ```
    jdbc.executeUpdatePreparedStatement(sql, parameters, function(err, results) {
      // Do Something.
    });
    ```
   
6. Executing a stored procedure. Note: the parameters array is an array of stored procedure parameter objects.
    
    - You can create the objects in this format:
        ```
        {
          type: <'in' or 'out'>,
          fieldName: <String>,
          dataType: <String from sql types constants property>,
          value: <any type>
        }
        ```
   
    - Or use the convenience functions:
        ```
        var inputParameter = jdbc.createSPInputParameter(value);
        var outputParameter = jdbc.createSPOutputParameter(sqlDataType, fieldName);
        ```
   
    - Execute the statement:
        ```
        jdbc.executeStoredProcedure(sql, parameters, function(err, result) {
          // Do Something.
        });
        ```
  
    - Note: The result object is a key value object where the keys are the output parameter field names.
        ```
        {
          <field name 1> : <output param value 1>,
          <field name 2> : <output param value 2>,
          <field name 3> : <output param value 3>,
        }
        ```
      
    - Note: If you stored procedure returns a 1 or more result sets you can access them through the result objects resultSets property. The resultsSetsProperty is an array of arrays where each array is a single result set:
        ```
        {
          resultSets: [
            [],
            []
          ]
        }
        ```
