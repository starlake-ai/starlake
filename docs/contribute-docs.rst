How to contribute to this documentation
=======================================

What is used
------------

This documentation is rendered by `sphinx <http://www.sphinx-doc.org/en/master/index.html>`_ and has to be written in reStructuredText (**rst**) markup language.
This choice has been strengthened by its plugin ecosystem.

How to render locally
----------------------
In order to render this documentation on your side, please install python package and plantuml.

On debian like:

.. code-block:: bash

    apt-get install python-pip \
                    python-setuptools \
                    plantuml

This documentation can be rendered on your side by following these steps:
 - *cd <COMET_APP_HOME>/docs*
 - *pip install -r requirements.txt*
 - *make clean html* or *make.bat clean html*

The last command will allow you to open **docs/\_build/html/index.html** in your favorite browser to see how beautiful your contribution is!
