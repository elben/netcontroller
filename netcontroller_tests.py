import time
import unittest
from netcontroller import *

CONFIG = """
    {
      "procs": [
        {"ip": "127.0.0.1", "port": 10000},
        {"ip": "127.0.0.1", "port": 10001},
        {"ip": "127.0.0.1", "port": 10002}
      ]
    }
    """

class TestNetController(unittest.TestCase):
    def setUp(self):
        self.nc0 = NetController(0, config_data=CONFIG)
        self.nc1 = NetController(1, config_data=CONFIG)
        self.nc2 = NetController(2, config_data=CONFIG)

    def tearDown(self):
        self.nc0.shutdown()
        self.nc1.shutdown()
        self.nc2.shutdown()

    def test_send_all_include_self(self):
        # Send to all, including to self
        self.nc0.send_all("send to all", False)
        self.nc0.send_all("send to all again", False)
        self.nc0.send_all("again and again and again", False)

        time.sleep(1)

        # Check each net controller got right messages

        addr, payload = self.nc0.next()
        payload = json.loads(payload)
        self.assertEquals(payload['to_proc'], 0)
        self.assertEquals(payload['from_proc'], 0)
        self.assertEquals(payload['msg'], 'send to all')

        addr, payload = self.nc0.next()
        payload = json.loads(payload)
        self.assertEquals(payload['msg'], 'send to all again')
        addr, payload = self.nc0.next()
        payload = json.loads(payload)
        self.assertEquals(payload['msg'], 'again and again and again')

        addr, payload = self.nc1.next()
        payload = json.loads(payload)
        self.assertEquals(payload['to_proc'], 1)
        self.assertEquals(payload['from_proc'], 0)
        self.assertEquals(payload['msg'], 'send to all')

        addr, payload = self.nc1.next()
        payload = json.loads(payload)
        self.assertEquals(payload['msg'], 'send to all again')
        addr, payload = self.nc1.next()
        payload = json.loads(payload)
        self.assertEquals(payload['msg'], 'again and again and again')

    def test_send_all_exclude_self(self):
        # Send to all, excluding self
        self.nc0.send_all("send to all")
        self.nc0.send_all("send to all again")
        self.nc0.send_all("again and again and again")

        time.sleep(1)

        # Check each net controller got right messages

        data_recv = self.nc1.next()
        self.assertNotEqual(data_recv, None)

        addr, payload = data_recv
        payload = json.loads(payload)
        self.assertEquals(payload['to_proc'], 1)
        self.assertEquals(payload['from_proc'], 0)
        self.assertEquals(payload['msg'], 'send to all')

        addr, payload = self.nc1.next()
        payload = json.loads(payload)
        self.assertEquals(payload['msg'], 'send to all again')

        addr, payload = self.nc1.next()
        payload = json.loads(payload)
        self.assertEquals(payload['msg'], 'again and again and again')

        self.assertEqual(self.nc0.next(), None)

    def test_send(self):
        # No messages in queue
        self.assertEqual(self.nc0.next(), None)
        self.assertEqual(self.nc1.next(), None)

        # Send stuff
        self.nc0.send(1, "test1")
        self.nc0.send(1, "test2")
        self.nc1.send(0, "test3")
        self.nc1.send(0, "test4")

        time.sleep(1)

        # Check each net controller got right messages

        addr, payload = self.nc0.next()
        payload = json.loads(payload)
        self.assertEquals(payload['to_proc'], 0)
        self.assertEquals(payload['from_proc'], 1)
        self.assertEquals(payload['msg'], 'test3')

        addr, payload = self.nc0.next()
        payload = json.loads(payload)
        self.assertEquals(payload['to_proc'], 0)
        self.assertEquals(payload['from_proc'], 1)
        self.assertEquals(payload['msg'], 'test4')

        addr, payload = self.nc1.next()
        payload = json.loads(payload)
        self.assertEquals(payload['to_proc'], 1)
        self.assertEquals(payload['from_proc'], 0)
        self.assertEquals(payload['msg'], 'test1')

        addr, payload = self.nc1.next()
        payload = json.loads(payload)
        self.assertEquals(payload['to_proc'], 1)
        self.assertEquals(payload['from_proc'], 0)
        self.assertEquals(payload['msg'], 'test2')
        
        # We should run out of messages
        self.assertEqual(self.nc0.next(), None)
        self.assertEqual(self.nc1.next(), None)

class TestConfig(unittest.TestCase):
    def setUp(self):
        self.config = Config(config_data=CONFIG)

    def test_procs(self):
        procs = self.config.procs()
        self.assertEquals(procs[0], {"ip": "127.0.0.1", "port": 10000})
        self.assertEquals(procs[1], {"ip": "127.0.0.1", "port": 10001})
        self.assertEquals(procs[2], {"ip": "127.0.0.1", "port": 10002})

    def test_proc(self):
        self.assertEquals(self.config.proc(0), {"ip": "127.0.0.1", "port": 10000})
        self.assertEquals(self.config.proc(1), {"ip": "127.0.0.1", "port": 10001})
        self.assertEquals(self.config.proc(2), {"ip": "127.0.0.1", "port": 10002})
        self.assertRaises(Exception, self.config.proc, (3,))

    def test_num_procs(self):
        self.assertEquals(self.config.num_procs(), 3)
    
    def test_add(self):
        self.config.add("localhost", 3333)
        self.assertEquals(self.config.proc(3), {"ip": "localhost", "port": 3333})
        self.config.add("elbenshira.com", 80)
        self.assertEquals(self.config.proc(4), {"ip": "elbenshira.com", "port": 80})

    def test_remove(self):
        orig_num = self.config.num_procs()

        self.config.add("localhost", 3333, name="proc10")
        self.config.add("elbenshira.com", 80, name="proc11")

        self.config.remove("proc10")
        self.config.remove("proc11")

        self.assertEquals(self.config.num_procs(), orig_num)

    def test_exists(self):
        self.config.add("localhost", 3333, name="proc10")

        self.assertTrue(self.config.exists("proc10"))
        self.assertTrue(self.config.exists(0))

if __name__ == "__main__":
    unittest.main()
