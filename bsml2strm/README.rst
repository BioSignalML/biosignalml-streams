bsml2strm
=========

Performance is improved by around 20% if Cython (http://cython.org/) is
installed and ``framestream.py`` compiled by running:::

  cython framestream.py

  # OS/X
  gcc -pthread -fPIC -fwrapv -O2 -Wall -fno-strict-aliasing  \
      -dynamiclib -undefined suppress -flat_namespace        \
      -I/usr/include/python2.7                               \
      -o framestream.so framestream.c

  # Linux
  gcc -pthread -fPIC -fwrapv -O2 -Wall -fno-strict-aliasing  \
      -shared                                                \
      -I/usr/include/python2.7                               \
      -o framestream.so framestream.c

adjusting as necessary the include path for ``Python.h``.
