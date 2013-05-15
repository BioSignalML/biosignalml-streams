import pyparsing as pp

"""
A language to connect BioSignalML recordings with telemetry streams.

Use of base URIs as per RFC 3986 (Uniform Resource Identifiers)


Future
~~~~~~

Parameter substition:

* positional and/or named parameters.
* parameters substituted before definitions are parsed.
* Formats:

  - {param}, {1}
  - $1, $10
  - Use backslash for escaping.


"""


## PyParsing grammer
_number = pp.Regex(r"\d+(\.\d*)?")
_number.setParseAction(lambda t:float(t[0]))

_string = (pp.Suppress('"') + pp.Word(pp.printables + ' ', excludeChars='"') + pp.Suppress('"')
         | pp.Suppress("'") + pp.Word(pp.printables + ' ', excludeChars="'") + pp.Suppress("'"))
_word   = pp.Word(pp.printables, excludeChars=' ,;<>[]"\'')

_yesno = (pp.CaselessLiteral('yes') ^ pp.CaselessLiteral('y')
         ^ pp.CaselessLiteral('no') ^ pp.CaselessLiteral('n'))
_yesno.setParseAction(lambda t:(t[0][0] == 'y'))

_path = _word
_uri  = pp.Combine('<' + _path + '>')
_pipe = pp.Word(pp.alphas + './', bodyChars=pp.printables, excludeChars=' ,;<>[]')

_quoted_word = (_word | pp.Suppress('"') + _word + pp.Suppress('"')
                      | pp.Suppress("'") + _word + pp.Suppress("'"))


_units = pp.Group(pp.CaselessKeyword('units') + pp.Suppress('=') + (_uri ^ _quoted_word))
_rate  = pp.Group(pp.CaselessKeyword('rate') + pp.Suppress('=') + _number)
_interval = pp.Group(pp.CaselessKeyword('segment') + pp.Suppress('=')
                   + pp.Group(_number + (pp.Literal('-') ^ pp.Literal(':')) + _number))
_binary   = pp.Group(pp.CaselessKeyword('binary')
                   + pp.Optional(pp.Suppress('=') + _yesno, default=True))
_stream_meta = pp.Group(pp.CaselessKeyword('stream_meta')
                   + pp.Optional(pp.Suppress('=') + _yesno, default=True))

_options  = (_rate | _units | _interval | _binary | _stream_meta)

_desc = pp.Group(pp.CaselessKeyword('desc') + pp.Suppress('=') + _string)
_label = pp.Group(pp.CaselessKeyword('label') + pp.Suppress('=') + (_string ^ _word))


_output_options = _options + pp.Optional(',').suppress()
_output_signal  = pp.Group(_uri + pp.Optional(_units) + pp.Optional(',').suppress())
_output_signals = pp.Group(pp.CaselessKeyword('signals')
                         + pp.Optional('=').suppress()
                         + pp.Group(pp.Suppress('[') + pp.ZeroOrMore(_output_signal) + pp.Suppress(']')))
_output_statement = pp.Group(pp.CaselessKeyword('stream')
                          + pp.Group(_uri
                                   + pp.CaselessKeyword('to').suppress() + _pipe
                                   + pp.ZeroOrMore(_output_options))
                          + _output_signals)


_input_options = (_options | _desc | _label) + pp.Optional(',').suppress()
_input_sigopts = (_units | _label | _desc) + pp.Optional(',').suppress()
_input_signal  = pp.Group(_uri + pp.ZeroOrMore(_input_sigopts))
_input_signals = pp.Group(pp.CaselessKeyword('signals')
                        + pp.Optional('=').suppress()
                        + pp.Group(pp.Suppress('[') + pp.ZeroOrMore(_input_signal) + pp.Suppress(']')))
# Following based on line 3609 of pyparsing.py
_turtle = (pp.empty.copy() + pp.CharsNotIn('[' + ']' + ' \t\r'))
_input_metadata = pp.Group(pp.CaselessKeyword('metadata')
                         + pp.Optional('=').suppress()
                         + pp.nestedExpr('[', ']', _turtle))

_input_statement = pp.Group(pp.CaselessKeyword('recording')
                          + pp.Group(_uri
                                   + pp.CaselessKeyword('from').suppress() + _pipe
                                   + pp.ZeroOrMore(_input_options))
                          + _input_signals
                          + pp.Optional(_input_metadata))


_statement = (_input_statement | _output_statement) + pp.Optional(pp.oneOf('; ,')).suppress()
_grammer   = pp.ZeroOrMore(_statement)



def _join_turtle(m):
#-------------------
  r = []
  for t in m:
    if isinstance(t, pp.ParseResults):
      r.append('[')
      r.append(_join_turtle(t))
      r.append(']')
    else:
      if len(r):
        if (r[-1] == "''" and t[0] == "'"
         or r[-1] == '""' and t[0] == '"'): r[-1] += t
        else: r.append(t)
      else: r.append(t)
  return ' '.join(r)



def parse(definition):
#=====================
  try:
    parsed = _grammer.parseString(definition, parseAll=True)
    for p in parsed:
      sigmeta = dict(p[2:])
      yield (p[0], p[1],  sigmeta.get('signals', []), _join_turtle(sigmeta.get('metadata', [])))

  except pp.ParseException as err:
    lines = definition.split('\n')
    errlines = '\n'.join(lines[err.lineno-1:err.lineno+2])
    raise ValueError("Syntax error in definition: \n%s" % errlines)


if __name__ == '__main__':
#=========================

  defn = """stream <http://devel.biosignalml.org/testdata/sinewave>
      to /tmp/pipe
      rate = 100, segment = 10:5
      units = mV
      binary
     signals [
      <signal/0>
      ] ,
    stream <http://devel.biosignalml.org/testdata/sinewave>
      to /tmp/pipe1
      segment = 10-20.7,
      stream_meta = no
     signals [
      <signal/0> units=<http://www.sbpax.org/uome/list.owl#Millivolt>
      <signal/0> units=mV
      ]
    recording <http://devel.biosignalml.org/testdata/sinewave>
      from /tmp/pipe2
      rate = 3
      units = 'mV'
      desc="An example"
     signals=[
       <signal/0> label="abc x" desc="hgv hgv j" units=mV
       ] ;
    recording <http://devel.biosignalml.org/testdata/sinewave2>
      from /tmp/pipe3
      units=<my/units> rate = 10
     signals [
      <signal/0> units=mV,
      <s3>
      ]
    metadata [
      @prefix : <http://example.org/stuff/1.0/> .

      :a :b "The first line\nThe second line\n  more" .

      :a :c [
        :d :e '''The first line
      The second line
        more''' ] .
    
      ]  
    """

  for r in parse(defn):
    print r[0]
    print ' ', r[1][0], r[1][1]
    print '   ', dict(r[1][2:])
    for sig in r[2]:
      print '   ', sig[0], dict(sig[1:])
    print ''
    print '   ', r[3]
    print ''
