import unittest

from manager import DHT_NONE, DHT_READY, FREE, LEADER, Manager


class DummySocket:
    pass


class ManagerProtocolTests(unittest.TestCase):
    def setUp(self):
        self.manager = Manager(0, sock=DummySocket())
        for index, name in enumerate(("alice", "bob", "carol", "dave")):
            reply = self.manager.dispatch([
                "register", name, "127.0.0.1",
                str(39501 + index * 2), str(39502 + index * 2),
            ])
            self.assertEqual(reply[:2], ["SUCCESS", "register"])

    def build_three_peer_dht(self):
        reply = self.manager.dispatch(["setup-dht", "alice", "3", "1950"])
        self.assertEqual(reply[:2], ["SUCCESS", "setup-dht"])
        self.assertEqual(self.manager.dispatch(
            ["dht-complete", "alice"]), ["SUCCESS", "dht-complete"])

    def test_registration_rejects_duplicate_endpoint(self):
        reply = self.manager.dispatch(
            ["register", "eve", "127.0.0.1", "39501", "39520"])
        self.assertEqual(reply[0], "FAILURE")

    def test_full_manager_lifecycle(self):
        self.build_three_peer_dht()
        free_peer = next(name for name, peer in self.manager.peers.items()
                         if peer["state"] == FREE)
        self.assertEqual(self.manager.dispatch(
            ["query-dht", free_peer])[0], "SUCCESS")

        leaver = self.manager.dht_members[1]
        self.assertEqual(self.manager.dispatch(
            ["leave-dht", leaver]), ["SUCCESS", "leave-dht"])
        new_leader = self.manager.pending_members[0]
        self.assertEqual(self.manager.dispatch(
            ["dht-rebuilt", leaver, new_leader]), ["SUCCESS", "dht-rebuilt"])
        self.assertEqual(self.manager.peers[leaver]["state"], FREE)
        self.assertEqual(self.manager.peers[new_leader]["state"], LEADER)

        self.assertEqual(self.manager.dispatch(
            ["join-dht", leaver])[0], "SUCCESS")
        self.assertEqual(self.manager.dispatch(
            ["dht-rebuilt", leaver, leaver]), ["SUCCESS", "dht-rebuilt"])
        self.assertEqual(self.manager.dht_status, DHT_READY)

        self.assertEqual(self.manager.dispatch(
            ["teardown-dht", leaver]), ["SUCCESS", "teardown-dht"])
        self.assertEqual(self.manager.dispatch(
            ["teardown-complete", leaver]), ["SUCCESS", "teardown-complete"])
        self.assertEqual(self.manager.dht_status, DHT_NONE)
        self.assertTrue(all(peer["state"] == FREE
                            for peer in self.manager.peers.values()))

    def test_busy_manager_rejects_unexpected_command(self):
        self.manager.dispatch(["setup-dht", "alice", "3", "1950"])
        reply = self.manager.dispatch(["query-dht", "dave"])
        self.assertEqual(reply[0], "FAILURE")
        self.assertIn("dht-complete", reply[2])


if __name__ == "__main__":
    unittest.main()
