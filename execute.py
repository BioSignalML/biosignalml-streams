

import command_processor



if __name__ == '__main__':
#=========================

  if len(sys.argv) < 2:
    sys.exit("Usage: %s [options] COMMAND_FILE [params]" % sys.argv[0])

  cmd = 1     # Position of command file in argument list...
  ## options:  -n      Print commands but without executing them
  ##           ??      Set working directory ??
  ##           ??      Set controlled directory ??

  ## params are expanded in COMMAND_FILE

  exitcode = 0

  for c in commands(open(sys.argv[cmd], 'r'), sys.argv[cmd:]):
    print 'C:', c.controlled_files()
    exitcode = c.run()

  sys.exit(exitcode)
