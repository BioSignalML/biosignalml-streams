bsml2strm
=========

Performance is improved by install Cython (http://cython.org/) and running:::

  cython framestream.py

  # OS/X
  gcc -dynamiclib -pthread -fPIC -fwrapv -O2 -Wall -fno-strict-aliasing  \
      -undefined suppress -flat_namespace -I/usr/include/python2.7       \
      -o framestream.so framestream.c

  # Linux
  gcc -shared -pthread -fPIC -fwrapv -O2 -Wall -fno-strict-aliasing  \
      -I/usr/include/python2.7                                       \
      -o framestream.so framestream.c

using the appropriate `-I` path to `Python.h`.
