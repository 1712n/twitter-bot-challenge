import unittest

from bot.message_formatter import volume_to_rate


class MyTestCase(unittest.TestCase):
    def test_common_volume_to_rate(self):
        volumes = {"A": 60, "B": 20, "C": 10, "D": 7, "E": 2, "F": 1}
        expected_rates = {"A": 60., "B": 20., "C": 10., "D": 7., "E": 2., "others": 1.}
        actual_rates = volume_to_rate(volumes)
        self.assert_rates_equals(expected_rates, actual_rates)

    def test_top_5(self):
        volumes = {"A": 60, "B": 20, "C": 10, "D": 7, "E": 3}
        expected_rates = {"A": 60., "B": 20., "C": 10., "D": 7., "E": 3.}
        actual_rates = volume_to_rate(volumes)
        self.assert_rates_equals(expected_rates, actual_rates)

    def test_top_3(self):
        volumes = {"A": 60, "B": 20, "C": 20}
        expected_rates = {"A": 60., "B": 20., "C": 20.}
        actual_rates = volume_to_rate(volumes)
        self.assert_rates_equals(expected_rates, actual_rates)

    def assert_rates_equals(self, expected, actual):
        for k, v in actual.items():
            self.assertTrue(k in expected)
            expected_rate = expected[k]
            self.assertTrue(float_equals(v, expected_rate))

        for k in expected.keys():
            self.assertTrue(k in actual)


def float_equals(x: float, y: float) -> bool:
    return abs(x - y) <= 0.000001


if __name__ == '__main__':
    unittest.main()
