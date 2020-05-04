from unittest import TestCase

from etcd3.endpoint import Endpoint


class TestEndpoint(TestCase):
    def setUp(self):
        self.endpoint = Endpoint()

    def test_that_used_endpoint_is_in_use(self):
        self.endpoint.use()
        self.assertTrue(self.endpoint.in_use)

    def test_that_failed_endpoint_is_not_in_use(self):
        self.endpoint.fail()
        self.assertFalse(self.endpoint.in_use)

    def test_that_value_error_occurs_if_failed_endpoint_is_used(self):
        self.endpoint.fail()
        self.assertRaises(ValueError, self.endpoint.use)
