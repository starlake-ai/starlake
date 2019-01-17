How to contribute to Comet Data Pipeline
========================================

This guide documents the best way to make various types of contribution to Comet Data Pipeline, including what is required before submitting a code change.

Contributing to Comet Data Pipeline doesn’t just mean writing code. Testing the application and improving documentation are also welcome.

Contribution for Ebiznext consultants
*************************************

In addition to this contribution guide, please refer to this document_.

.. _document: https://docs.google.com/document/d/1BXZ92PyEajNBXy1DTbsiKqWF52qJmH_U89WGaOCAeMk/edit?usp=sharing

Contributing Issue Reports
**************************

Issue can be of many kind:
* new feature request
* bug report
* documentation

Bug reports are only useful however if they include enough information to understand, isolate and ideally reproduce the bug. Simply encountering an error does not mean a bug should be reported. Search issues before creating it. Unreproducible bugs, or simple error reports, may be closed.

They have to be reported by creating an issue based on and following the appropriate template.

The issue life cycle is:
* an issue is created, following the appropriate template
* if any clarification is needed, a technical exchange will follow in the issue comments
* when the issue is considered to be clear enough by at least 2 animators of the project, the issue is flagged as "ready"
* for Ebiznext consultant, after that point, please refer to this document_.

.. _document: https://docs.google.com/document/d/1BXZ92PyEajNBXy1DTbsiKqWF52qJmH_U89WGaOCAeMk/edit?usp=sharing


Contributing to architecture documentation
******************************************

Architecture documents will be exposed by read the docs, in `.rst` format, in the `doc/architecture` folder.
Any architecture changes, update or proposal can be made through a documentation issue, followed by a ``doc/`` pull request (c.f. below).

Contributing by Reviewing Changes
*********************************
Changes to Comet Data Pipeline source code are proposed, reviewed and committed via Github merge requests (described later). Anyone can view and comment on active changes here. Reviewing others’ changes is a good way to learn how the change process works and gain exposure to activity in various parts of the code. You can help by reviewing the changes and asking questions or pointing out issues – as simple as typos or small issues of style.

The Review Process
==================

* Other reviewers, including committers, may comment on the changes and suggest modifications. Changes can be added by simply pushing more commits to the same branch.
* Lively, polite, rapid technical debate is encouraged from everyone. The outcome may be a rejection of the entire change.
* Reviewers can indicate that a change looks suitable for merging with a comment.
* Sometimes, other changes will be merged which conflict with your pull request’s changes. The merge request can’t be merged until the conflict is resolved.
* Try to be responsive to the discussion rather than let days pass between replies.

Contributing Documentation Changes
**********************************
To propose a change to documentation, you have to create an issue first, then edit the Sphinx source files in Comet Data Pipeline’s docs/ directory and try to build it by following the process described in ``building.rst``. The process to propose a doc change is otherwise the same as the process for proposing code changes below.


Contributing Code Changes
*************************

Code changes are all related to an issue. If you plan to contribute code changes, please read carrefully the following sections.

Git workflow
============

Comet Data Pipeline source code has the following branches' type:

* ``master`` is where all issues are merged to and it is the only way to commit to master
* ``[X.Y].x`` is a release branch branched from ``master`` Each commits on it are related to a fix branch.
* ``dev/CDP-[NUM]`` is a development branch branched from ``master`` related to an issue with the id [NUM] with an improvement purpose.
* ``fix/CDP-[NUM]`` is a development branch branched from ``[X.Y].x`` related to an issue with the id [NUM] and where ``[X.Y].x`` is an **active branch** and the **oldest release** affected by the issue. Fix branches are merged to the affected release and to master.
* ``doc/CDP-[NUM]`` is a documentation branch branched from ``master`` or ``[X.Y].x`` related to an issue with the id [NUM]. If it's a **release branch**, it has to be the **oldest release possible**.

.. highlight:: none

.. code-block::

              o--o--    fix/CDP-101
             /
         o--o--o--      1.0.0
        /       \
    o--o--o--o---o--o-- master
     \        \
      \        o--o--   doc/CDP-99
       o--o--           dev/CDP-100

Merge Request
=============

#. Create a new branch following the pattern listed above
#. Commit your changes. It has to match the following pattern: ``[CDP-[NUM]]: [My message]`` where [NUM] is the related issue number. E.g: ``[CDP-42]: My greatest commit``
#. Run ``test`` SBT task
#. Run ``scalafmt`` SBT task
#. Push commits to your branch
#. Open a merge request against the branch you branched from.
    * The merge request's title has to match the following pattern: ``[CDP-[NUM]]: [My message]`` where [NUM] is the related issue number. E.g: ``[CDP-42]: My great contribution``.
    * The pull request's body has to include: "closes #[NUM]" where [NUM] is the related issue number. 
    * If the merge request is in progress, please add ``WIP:`` in front of the title.
#. Jenkins automatic merge request builder will test your changes
#. Jenkins will update the pipeline status of the merge request.
#. Watch for the results, and investigate and fix failures promptly.
    #. Fixes can simply be pushed to the same branch from which you opened your merge request
    #. Jenkins will automatically re-test when new commits are pushed
    #. If the tests failed for reasons unrelated to the change (e.g. Jenkins outage), then a committer can request a re-test with “retest”.

Code Style Guide
****************
* Comet Data Pipeline uses ``scalafmt`` to format scala codes. Configuration is located in the file ``.scalafmt`` at the root of the project. Use ``scalafmt`` SBT task to format your code and use ``scalafmtCheck`` SBT task to validate your code.
* Git commit log is linted with ``gitlint``. Configuration is located in the file ``.gitlint``.
    * Use ``gitlint --commits master..HEAD`` to validate your commit log.
    * You can install commit-msg hook with ``gitlint install-hook``. It will prevent you from committing with a wrong message.