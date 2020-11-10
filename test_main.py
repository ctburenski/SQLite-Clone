import pytest
import subprocess
import select

"""
Entire program depends on the process being called
to not use line buffering, otherwise it will never
return from a readline() call.
"""


def prepare():
  proc = subprocess.Popen('./main', bufsize=0, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          stdin=subprocess.PIPE, text=True)
  return proc


def send_data(pipe, data):
  pipe.write(data + '\n')
  pipe.flush()


def read_out(pipe):
  # the poll() ensures we don't hang for no reason.
  p = select.poll()
  p.register(pipe, select.POLLIN)
  # Ideally, this will never need to wait that long.
  # This mostly affects the test_table_limit() scenario
  if p.poll(600):
    return pipe.readline().strip()


def test_insert():  
  proc = prepare()
  data = "insert 1 cody ctb@gmail.com"
  data_result = "db> (1, cody, ctb@gmail.com)"

  send_data(proc.stdin, data)
  assert "db> Executed." == read_out(proc.stdout)

  # ending the program before reading the final line prevents blocking
  # since the prompt has no newlines or EOF at the end.
  send_data(proc.stdin, ".exit")
  # would otherwise be "db> " but the strip() call during read_out() removes the space
  assert "db>" == read_out(proc.stdout)

  proc.wait()


def test_select():
  proc = prepare()
  data = '1 cody ctb@gmail.com'
  data_result = 'db> (1, cody, ctb@gmail.com)'

  send_data(proc.stdin, "insert " + data)
  assert "db> Executed." == read_out(proc.stdout)

  send_data(proc.stdin, "select")
  assert data_result == read_out(proc.stdout)

  send_data(proc.stdin, ".exit")
  # if done before exit, the following assert will
  # sometimes hang for no obvious reason
  # probably a problem with the C program flushing
  assert "Executed." == read_out(proc.stdout)
  assert "db>" == read_out(proc.stdout)

  proc.wait()


def test_fail_insert():
  proc = prepare()

  send_data(proc.stdin, "insert")
  assert "db> Syntax error. Could not parse statement." == read_out(proc.stdout)
  
  send_data(proc.stdin, ".exit")
  assert "db>" == read_out(proc.stdout)

  proc.wait()


def test_blank_line():
  proc = prepare()

  send_data(proc.stdin, "Y")
  assert "db> Unrecognized keyword at start of 'Y'." == read_out(proc.stdout)

  send_data(proc.stdin, ".exit")
  assert "db>" == read_out(proc.stdout)

  proc.wait()


def test_table_limit():
  proc = prepare()

  for i in range(1, 1301):
    send_data(proc.stdin, "insert {0} name{0} email{0}".format(i))
    assert "db> Executed." == read_out(proc.stdout)
  
  send_data(proc.stdin, "insert 1400 name email")
  assert "db> Error: table full." == read_out(proc.stdout)

  send_data(proc.stdin, ".exit")
  assert "db>" == read_out(proc.stdout)

  proc.wait()


def test_prompt():
  proc = prepare()

  send_data(proc.stdin, ".exit")
  assert "db>" == read_out(proc.stdout)

  proc.wait()


def test_max_length():
  proc = prepare()

  long_name = "a"*32
  long_email = "b"*255
  data = "insert 1 " + long_name + " " + long_email
  result_data = "db> (1, {0}, {1})".format(long_name, long_email)

  send_data(proc.stdin, data)
  assert "db> Executed." == read_out(proc.stdout)

  send_data(proc.stdin, "select")
  assert result_data == read_out(proc.stdout)

  send_data(proc.stdin, ".exit")
  assert "Executed." == read_out(proc.stdout)
  assert "db>" == read_out(proc.stdout)

  proc.wait()


# will be getting more robust error handling
def test_beyond_max_length():
  proc = prepare()

  long_name = "a"*33
  long_email = "b"*256
  data = "insert 1 " + long_name + " " + long_email
  result_data = "db> (1, {0}, {1})".format(long_name, long_email)

  send_data(proc.stdin, data)
  assert "db> Executed." == read_out(proc.stdout)

  send_data(proc.stdin, "select")
  assert result_data != read_out(proc.stdout)

  send_data(proc.stdin, ".exit")
  assert "Executed." == read_out(proc.stdout)
  assert "db>" == read_out(proc.stdout)

  proc.wait()
