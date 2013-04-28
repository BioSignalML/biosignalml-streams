import pyparsing as pp

## PyParsing grammer
_number = pp.Regex(r"\d+(\.\d*)?")
_number.setParseAction(lambda t:float(t[0]))

_yesno = (pp.CaselessLiteral('yes') ^ pp.CaselessLiteral('y')
         ^ pp.CaselessLiteral('no') ^ pp.CaselessLiteral('n'))
_yesno.setParseAction(lambda t:(t[0][0] == 'y'))

_path = pp.Word(pp.printables, excludeChars=' ,;<>[]')
_uri  = pp.Combine('<' + _path + '>')
_pipe = pp.Word(pp.alphas + './', bodyChars=pp.printables, excludeChars=' ,;<>[]')

_units = pp.Group(pp.CaselessKeyword('units') + pp.Suppress('=') + (_uri ^ _path))
_rate  = pp.Group(pp.CaselessKeyword('rate') + pp.Suppress('=') + _number)
_interval = pp.Group(pp.CaselessKeyword('segment') + pp.Suppress('=')
                   + pp.Group(_number + (pp.Literal('-') ^ pp.Literal(':')) + _number))
_binary   = pp.Group(pp.CaselessKeyword('binary')
                   + pp.Optional(pp.Suppress('=') + _yesno, default=True))
_metadata = pp.Group(pp.CaselessKeyword('metadata')
                   + pp.Optional(pp.Suppress('=') + _yesno, default=True))

_options  = (_rate | _units | _interval | _binary | _metadata) + pp.Suppress(pp.Optional(','))

#_recording = pp.Group(_uri + pp.Optional(_pipe, default='-') + pp.ZeroOrMore(_options))


_signal  = pp.Group(_uri + pp.Optional(_units) + pp.Suppress(pp.Optional(',')))
_signals = pp.Group(pp.Suppress('[') + pp.ZeroOrMore(_signal) + pp.Suppress(']'))

_output_statement = pp.Group(pp.CaselessKeyword('stream')
                          + pp.Group(_uri
                                   + pp.Optional(pp.CaselessKeyword('to').suppress() + _pipe,
                                                            default='stdout')
                                   + pp.ZeroOrMore(_options))
                          + _signals)


# _string = 
# _label = pp.Group(pp.CaselessKeyword('label') + pp.Suppress('=') + _string)

#_inopts = (_rate | _units) + pp.Suppress(pp.Optional(','))
#_input  = pp.Group(_uri + pp.ZeroOrMore(_inopts) + pp.Suppress(pp.Optional(',')))
#_inputs = pp.Group(pp.Suppress('[') + pp.ZeroOrMore(_input) + pp.Suppress(']'))

_input_statement = pp.Group(pp.CaselessKeyword('recording')
                          + pp.Group(_uri
                                   + pp.Optional(pp.CaselessKeyword('from').suppress() + _pipe,
                                                            default='stdin')
                                   + pp.ZeroOrMore(_options))
                          + _signals)

_statement = (_input_statement | _output_statement) + pp.Suppress(pp.Optional(pp.oneOf('; ,')))
_grammer   = pp.ZeroOrMore(_statement)



def parse(definition):
#=====================
  try:
    return _grammer.parseString(definition, parseAll=True)
  except pp.ParseException as err:
    raise ValueError(err)


if __name__ == '__main__':
#=========================

  defn = """stream <http://devel.biosignalml.org/testdata/sinewave>
      to /tmp/pipe
      rate = 100, segment = 10:5
      units = mV
      binary
     [
      <signal/0>
      ] ,
    stream <http://devel.biosignalml.org/testdata/sinewave>
      segment = 10-20.7,
      metadata = no
     [
      <signal/0> units=<http://www.sbpax.org/uome/list.owl#Millivolt>
      <signal/0> units=mV
      ]
    recording <http://devel.biosignalml.org/testdata/sinewave> from /tmp/pipe rate = 3 [
      <signal/0>
      ] ;
    recording <http://devel.biosignalml.org/testdata/sinewave2> units=<my/units> rate = 10 [
      <signal/0> units=mV,
      <s3>
      ]"""

  for r in parse(defn):
    print r[0]
    print ' ', r[1][0], r[1][1]
    print '   ', dict(r[1][2:])
    for sig in r[2]:
      print '   ', sig[0], dict(sig[1:])

