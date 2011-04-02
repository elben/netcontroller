import socket
from threading import Thread
import json
import Queue

class NetController:
    """
    UDP. Async, thread-safe queues. Ghetto-simple configuration.
    What else could you ask for? (Bug-freeness, maybe.)

    Author: Elben Shira <elbenshira@gmail.com>

    Configuration is in JSON format. Here is an example:

        {
          "procs": [
            {"ip": "127.0.0.1", "port": 10000},
            {"ip": "127.0.0.1", "port": 10001},
            {"ip": "127.0.0.1", "port": 10002}
          ]
        }

    For usage example, check out netcontroller_tests.py.

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
        self.listener = ListenServer(self.shared_state, self.queue, self.config.proc(self.proc_id)['port'])
        self.listener.start()

    def send(self, proc_id, msg):
        """
        Sends a message to proc_id.
        """

        payload = self._create_msg(proc_id, msg)
        self.sock.sendto(payload, self.config.proc_addr(proc_id))

    def send_all(self, msg, exclude_self=True):
        """
        Sends a message to all known procs.
        If exclude_self is True, send it to ourself too.
        """

        for proc_id, proc in enumerate(self.config.proc_addrs()):
            if exclude_self and proc_id == self.proc_id:
                continue
            payload = self._create_msg(proc_id, msg)
            self.sock.sendto(payload, tuple(proc))

    def next(self, block=False):
        """
        Returns the first (from_addr, msg) tuple in the queue.
        If `block` is True, this method will block until a message is available.
        """

        try:
            addr, payload = self.queue.get(block)
            return addr, json.loads(payload)
        except Queue.Empty:
            return None

    def shutdown(self):
        self.shared_state['alive'] = False
        self.listener.join()

    def _create_msg(self, to_proc, msg):
        payload = json.dumps({
            'from_proc': self.proc_id,
            'to_proc': to_proc,
            'msg': msg},)
        return payload

class ListenServer(Thread):
    def __init__(self, shared_state, queue, port, ip='127.0.0.1', timeout=1, *args, **kwargs):
        Thread.__init__(self, *args, **kwargs)  # must be called first
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(timeout)
        self.queue = queue
        self.ip = ip
        self.port = port
        self.shared_state = shared_state

    def run(self):
        self.sock.bind((self.ip, self.port))
        msg = None
        from_addr = None
        while self.shared_state['alive']:
            try:
                msg, from_addr = self.sock.recvfrom(4096)
                self.queue.put((from_addr, msg))
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

    def procs(self):
        return self._procs

    def proc_addrs(self):
        addrs = []
        for proc in self.procs():
            addrs.append((proc['ip'], proc['port']))
        return addrs

    def proc_addr(self, proc_id):
        proc = self.proc(proc_id)
        return (proc['ip'], proc['port'])

    def proc(self, proc_id):
        if len(self._procs) > proc_id:
            return self._procs[proc_id]
        else:
            raise Exception("No process with number %d." % p);

    def num_procs(self):
        return len(self._procs)

