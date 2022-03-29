dMyplant
========

Python Interface to INNIO MyPlant.

Easy access to INNIO Assets via Myplant API. Download Pandas Dataframes
on a predefined Validation Fleet. Calculate demonstrated Reliability
based on Success run Method and Lipson Equality approach as described in
A.Kleyners Paper "Reliability Demonstration in Product Validation
Testing".


Installation
------------

**Windows:**

install miniconda distribution https://conda.io/projects/conda/en/latest/index.html

*inside the Miniconda prompt:*
::

  git clone https://github.com/DieterChvatal/dmyplant4.git

*in a Miniconda command window, cd into the dmyplant2 folder and run*
::
  conda install "arrow=1.0.3"
  conda install pandas matplotlib scipy bokeh tabulate
  python setup.py develop

*this creates links to this package in the Anaconda package location.
to remove these links use*
::
  python setup.py develop --uninstall

*Now you can modify and extend the package in place ...*


Usage example
-------------

create an **input.csv** file with your myplant assets in your working directory, e.g.:
::
  n;Validation Engine;serialNumber;val start;oph@start;starts@start
  0;POLYNT - 2 (1145166-T241) --> Sept;1145166;12.10.2020;31291;378
  1;REGENSBURG;1175579;14.09.2020;30402;1351
  2;ROCHE PENZBERG KWKK;1184199;27.04.2020;25208;749
  3;ECOGEN ENERGY SYSTEMS BVBA;1198719;15.10.2020;28583;711
  4;BMW REGENSBURG M3;1243360;17.08.2020;63893;2016
  5;REGENSBURG;1243362;07.09.2020;62765;
  6;ABINSK;1250575;15.06.2020;758;
  7;PROSPERITY WEAVING MILLS LTD - 1 (1351388-X243);1250578;12.10.2020;0;352
  8;SOTERNIX RENOVE;1310773;25.09.2020;18439;1218
  9;BMW MÜNCHEN;1319133;31.08.2020;4532;581

create a python file **main.py** in your working directory:
::
  import dmyplant2
  import pandas as pd
  import numpy as np
  import logging
  import sys
  import traceback

  global DEBUG
  DEBUG = False

  logging.basicConfig(
      filename='dmyplant.log',
      filemode='w',
      format='%(asctime)s %(levelname)s, %(message)s',
      level=logging.INFO
  )
  hdlr = logging.StreamHandler(sys.stdout)
  logging.getLogger().addHandler(hdlr)


  def main():
      try:
          logging.info('---')
          logging.info('dMyplant demo app started')

          # load input data from files
          dval = dmyplant2.Validation.load_def_csv("input.csv")

          # ask & store credentials
          dmyplant2.cred()

          # myplant instance
          mp = dmyplant2.MyPlant(600) #parameter seconds to cache values e.g. 600 for 10 minutes or 0 to force reload

          # validation instance
          vl = dmyplant2.Validation(mp,dval, cui_log=False)

          # call dashboard
          d=vl.dashboard
          print('\nDashboard:')
          print(d, '\n')

          logging.info('dMyplant demo app completed.')
          logging.info('---')

      except Exception as e:
          print(e)
          if DEBUG:
              traceback.print_tb(e.__traceback__)
      finally:
          hdlr.close()
          logging.getLogger().removeHandler(hdlr)


  if __name__ == '__main__':
      main()
    
 
During the first run and every following 31 days, you are prompted for your myplant
login and credentials in the command window:
::
  Please enter your myPlant login:
  User: xxxxxxx
  Password: xxxxxxxx


basic help is available in the python REPL:
::
  import dmyplant2
  help(dmyplant2)

  or 

  help(dmyplant2.dEngine)


Release History
---------------

-  0.0.1
-  Work in progress

Meta
----

Your Name – dieter.chvatal@innio.com

Distributed under the MIT license. See ``LICENSE`` for more information.

`https://github.com/DieterChvatal/dmyplant2 <https://github.com/DieterChvatal/>`__


Contributing
------------

1. Fork it (https://github.com/DieterChvatal/dmyplant2)
2. Create your feature branch (``git checkout -b feature/fooBar``)
3. Commit your changes (``git commit -am 'Add some fooBar'``)
4. Push to the branch (``git push origin feature/fooBar``)
5. Create a new Pull Request

