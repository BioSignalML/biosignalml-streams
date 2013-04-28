Connection Definitions
----------------------

An example output stream:::

  stream <http://devel.biosignalml.org/testdata/sinewave>
    to pipe
    rate=1000
      [ <signal/0> ]


An example input stream:::

  recording <http://devel.biosignalml.org/testdata/new>
    from pipe
    rate = 100
      [ <sig1> units = 'mV' ]


Options:::

  rate = RATE

  segment = FROM-TO
  segment = FROM:DURATION

  units = <URI>
  units = SYMBOL

  binary = YES | NO

  label = WORD | STRING

  description = STRING
