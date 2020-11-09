***************************************************************************************************
transform | job
***************************************************************************************************


Synopsis
--------

**comet transform | job [options]**


Description
-----------




Options
-------

.. option:: --name: <value>

    *Required*. Job Name


.. option:: --views: <value>

    *Optional*. view1,view2 ...
    If present only the request present in these views statements are run. Useful for unit testing
    


.. option:: --views-dir: <value>

    *Optional*. Where to store the result of the query in JSON


.. option:: --views-count: <value>

    *Optional*. Max number of rows to retrieve. Negative value means the maximum value 2147483647


.. option:: --options: k1=v1,k2=v2...

    *Optional*. Job arguments to be used as substitutions


