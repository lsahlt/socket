import unittest

from protocol import (first_prime_after, fmt_record, fmt_tuple, hash_record,
                      parse_record, parse_tuple)


class ProtocolTests(unittest.TestCase):
    def test_tuple_round_trip(self):
        value = ("alice", "127.0.0.1", 39502)
        self.assertEqual(parse_tuple(fmt_tuple(*value)), value)

    def test_record_round_trip_preserves_commas(self):
        record = ["12", "STATE", "1950", "April", "Tornado", "C",
                  "A NAME, WITH COMMA", "0", "0", "0", "0", "1K", "0", "F1"]
        self.assertEqual(parse_record(fmt_record(record)), record)

    def test_hashing_matches_specification(self):
        self.assertEqual(first_prime_after(446), 449)
        self.assertEqual(hash_record(10096222, 449, 3),
                         (10096222 % 449, (10096222 % 449) % 3))


if __name__ == "__main__":
    unittest.main()
