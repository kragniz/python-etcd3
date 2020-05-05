from unittest import TestCase

from etcd3 import client
from etcd3.endpoint import Endpoint


class TestClient(TestCase):
    def setUp(self):
        self.client = client()


class TestInitEndpoints(TestClient):
    def test_initial_default_endpoints(self):
        endpoint = Endpoint("localhost", 2379, None, None, None)
        self.assertTrue(endpoint.netloc in self.client.endpoints.keys())
        self.assertEqual(self.client.endpoint_in_use.netloc, endpoint.netloc)

    def test_initial_endpoints(self):
        first_endpoint = Endpoint("localhost", 2379, None, None, None)
        second_endpoint = Endpoint("localhost", 2378, None, None, None)
        third_endpoint = Endpoint("localhost", 2377, None, None, None)
        endpoints = {"first": first_endpoint, "second": second_endpoint, "third": third_endpoint}
        self.client = client(endpoints=endpoints)

        self.assertTrue(first_endpoint in self.client.endpoints.values())
        self.assertTrue(second_endpoint in self.client.endpoints.values())
        self.assertTrue(third_endpoint in self.client.endpoints.values())
