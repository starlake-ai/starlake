.. _rls_concept:

RowLevelSecurity
##########################################

User / Group and Service accounts rights on a subset of the table.

.. option:: name: String

*Required*. This Row Level Security unique name.

.. option:: predicate: String

*Required*. The condition that goes to the WHERE clause and limit the visible rows. 
To make all the rows visible, this predicate should be equal to TRUE

.. option:: grants: List[String]

*Required*. user / groups / service accounts to which this security level is applied.
For example: user:me@mycompany.com,group:group@mycompany.com,serviceAccount:mysa@google-accounts.com
