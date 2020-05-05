from unittest import TestCase

from etcd3.endpoint import Endpoint


class TestEndpoint(TestCase):
    def setUp(self):
        self.endpoint = Endpoint()


class TestInit(TestEndpoint):
    def test_initial_host(self):
        self.assertEqual(self.endpoint.host, "localhost")

    def test_initial_netloc(self):
        self.assertEqual(self.endpoint.netloc, "localhost:2379".format(host="localhost", port=2379))

    def test_initial_credentials(self):
        self.assertEqual(self.endpoint.credentials, None)

    def test_initial_time_retry(self):
        self.assertEqual(self.endpoint.time_retry, 300.0)

    def test_initial_secure(self):
        self.assertEqual(self.endpoint.protocol, "http")


class TestEndpointUsage(TestEndpoint):
    def test_that_used_endpoint_is_in_use(self):
        self.endpoint.use()
        self.assertTrue(self.endpoint.in_use)

    def test_that_failed_endpoint_is_not_in_use(self):
        self.endpoint.fail()
        self.assertFalse(self.endpoint.in_use)

    def test_that_value_error_occurs_if_failed_endpoint_is_used(self):
        self.endpoint.fail()
        self.assertRaises(ValueError, self.endpoint.use)
