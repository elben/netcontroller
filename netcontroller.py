import socket
from threading import Thread
import json
import Queue


def build_payload(from_proc, to_proc, client_payload, **kwargs):
    """
    Builds a JSON payload.

    from_proc and to_proc can be anything (integer, str).
    client_payload should be a dict.
    """
    payload = {
        'from_proc': from_proc,
        'to_proc': to_proc,
        'msg': client_payload,
    }
    payload.update(kwargs)
    payload['msg'] = client_payload

    payload_json = json.dumps(payload)
    return payload_json

class NetController:
    """
    UDP. Async, thread-safe queues. Ghetto-simple configuration. JSON-based
    message passing. What else could you ask for? (Bug-freeness, maybe.)

    Author: Elben Shira <elbenshira@gmail.com>

    =====================
    CONFIGURATION
    =====================

    Configuration is in JSON format. You can either pass in a string or specify
    a path to a file.
    
    Here is an example:

        {
          "procs": [
            {"ip": "127.0.0.1", "port": 10000},
            {"ip": "127.0.0.1", "port": 10001},
            {"ip": "127.0.0.1", "port": 10002}
          ]
        }

    =====================
    USAGE
    =====================

    For usage example, check out netcontroller_tests.py.

    NetController passes everything as JSON-formatted messages. So when you call
    send(), NetController expects your payload to be a Python object that can be
    converted to JSON (e.g. int, string, dict).

    =====================
    WARNINGS & NOTES
    =====================
    
    The ListenServer thread is a daemon thread. This allows the Python
    interpreter to quit when only daemon threads are around.

    The default buffer size is 4096 bytes!

    Note that the tests assume we are testing on a local network, thus UDP packets
    are ordered. This library has not been testing in a non-local environment.
    """

    def __init__(self, proc_id, config_data=None, config_path='config.json'):
        self.config = Config(config_data, config_path)
        self.queue = Queue.Queue()
        self.proc_id = proc_id

        self.ip = self.config.proc(proc_id)['ip']
        self.port = self.config.proc(proc_id)['port']
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Spawn listener thread.
        self.shared_state = {'alive': True}
        self.listener = ListenServer(self.shared_state,
                self.queue,
                self.config.proc(self.proc_id)['port'],)
        self.listener.daemon = True
        self.listener.start()

    def send(self, to_proc, client_payload):
        """
        Sends a message to to_proc.
        """

        payload = build_payload(self.proc_id, to_proc, client_payload)
        self.sock.sendto(payload, self.config.proc_addr(to_proc))

    def send_all(self, msg, exclude_self=True, delay_func=None,
            start=0, num=None):
        """
        Sends a message to all known procs.
        If exclude_self is True, send it to ourself too.

        delay_func is a function that gets called before each sendto is called.
        start - start sending to this process first
        num - send to this many processes, starting from start. Defaults to
              None, which is all processes.
        """

        num_procs = self.config.num_procs()
        num = num if num else num_procs
        for i in range(start, start+num):
            to_proc = i % self.config.num_procs()
            proc = self.config.proc_addrs()[to_proc]

            if exclude_self and to_proc == self.proc_id:
                continue

            payload = build_payload(self.proc_id, to_proc, msg)

            if delay_func:
                delay_func()

            self.sock.sendto(payload, tuple(proc))

    def next(self, block=False, timeout=None, condition=None):
        """
        Returns the first (from_addr, msg) tuple in the queue.
          from_addr - a tuple (ip, port)
          msg - a string

        If `block` is True, this method will block until a message is available.
        Otherwise, returns None is no message is available.

        If blocking, the `timeout` is how long we wait for a message before
        timing out. By default, the timeout is infinite.

        `condition` is a function that takes one argument, the payload received.
        If `condition` returns True, we return the payload. Otherwise, we put
        the payload to the back of the queue and immediately return None.
        """

        try:
            addr, payload = self.queue.get(block, timeout)
            if condition is None or condition(payload):
                return addr, payload
            else:
                # Condition not met
                self.queue.put((addr, payload))
                return None
        except Queue.Empty:
            return None

    def shutdown(self):
        self.shared_state['alive'] = False
        self.listener.join()

class ListenServer(Thread):
    def __init__(self, shared_state, queue, port, ip='127.0.0.1', timeout=1,
            bufsize=4096, *args, **kwargs):
        Thread.__init__(self, *args, **kwargs)  # must be called first
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(timeout)
        self.queue = queue
        self.ip = ip
        self.port = port
        self.shared_state = shared_state
        self.bufsize = bufsize

    def run(self):
        self.sock.bind((self.ip, self.port))
        msg = None
        from_addr = None
        while self.shared_state['alive']:
            try:
                payload, from_addr = self.sock.recvfrom(self.bufsize)
                self.queue.put((from_addr, payload))
            except socket.timeout:
                pass
        self.sock.close()

class Config:
    def __init__(self, config_data=None, config_path='config.json'):
        self._procs = []
        if config_data:
            config_json = json.loads(config_data)
        else:
            config_json = json.load(open(config_path))

        if 'procs' in config_json:
            self._procs = config_json['procs']

    def add(self, ip, port):
        """
        Adds a new process (ip, port) into the system.
        """
        self._procs.append({'ip': ip, 'port': port})

    def remove(self, ip, port):
        """
        Removes process (ip, port) from system.
        """
        for i, p in enumerate(self._procs):
            if p['ip'] == ip and p['port'] == port:
                del self._procs[i]

    def procs(self):
        return self._procs

    def proc_addrs(self):
        addrs = []
        for proc in self.procs():
            addrs.append((proc['ip'], proc['port']))
        return addrs

    def proc_addr(self, proc_id):
        """
        Returns (ip, port) tuple.
        """
        proc = self.proc(proc_id)
        return (proc['ip'], proc['port'])

    def proc(self, proc_id):
        """
        Returns the raw process configuration, as seen in the JSON config file.
        """
        if len(self._procs) > proc_id:
            return self._procs[proc_id]
        else:
            raise Exception("No process with number %s." % str(proc_id));

    def num_procs(self):
        return len(self._procs)

