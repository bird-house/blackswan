.. _installation:

Installation
============

.. contents::
    :local:
    :depth: 1

Install from Conda
------------------

.. warning::

   TODO: Prepare Conda package.

Install from GitHub
-------------------

Check out code from the Blackswan GitHub repo and start the installation:

.. code-block:: sh

   $ git clone https://github.com/nkadygrov/blackswan.git
   $ cd blackswan
   $ conda env create -f environment.yml
   $ source activate blackswan
   $ python setup.py develop

... or do it the lazy way
+++++++++++++++++++++++++

The previous installation instructions assume you have Anaconda installed.
We provide also a ``Makefile`` to run this installation without additional steps:

.. code-block:: sh

   $ git clone https://github.com/nkadygrov/blackswan.git
   $ cd blackswan
   $ make clean    # cleans up a previous Conda environment
   $ make install  # installs Conda if necessary and runs the above installation steps

Start Blackswan PyWPS service
-----------------------------

After successful installation you can start the service using the ``blackswan`` command-line.

.. code-block:: sh

   $ blackswan --help # show help
   $ blackswan start  # start service with default configuration

   OR

   $ blackswan start --daemon # start service as daemon
   loading configuration
   forked process id: 42

The deployed WPS service is by default available on:

http://localhost:5000/wps?service=WPS&version=1.0.0&request=GetCapabilities.

.. NOTE:: Remember the process ID (PID) so you can stop the service with ``kill PID``.

You can find which process uses a given port using the following command (here for port 5000):

.. code-block:: sh

   $ netstat -nlp | grep :5000


Check the log files for errors:

.. code-block:: sh

   $ tail -f  pywps.log

... or do it the lazy way
+++++++++++++++++++++++++

You can also use the ``Makefile`` to start and stop the service:

.. code-block:: sh

  $ make start
  $ make status
  $ tail -f pywps.log
  $ make stop


Run Blackswan as Docker container
---------------------------------

You can also run Blackswan as a Docker container.

.. warning::

  TODO: Describe Docker container support.

Use Ansible to deploy Blackswan on your System
----------------------------------------------

Use the `Ansible playbook`_ for PyWPS to deploy Blackswan on your system.


.. _Ansible playbook: http://ansible-wps-playbook.readthedocs.io/en/latest/index.html
