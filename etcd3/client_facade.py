from etcd3 import client, exceptions
from etcd3.endpoint import Endpoint


class ClientFacade:
    def __init__(self, host='localhost', port=2379, endpoints=None,
                 ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
                 user=None, password=None, grpc_options=None, failover=False):
        self.client = client(host=host, port=port, endpoints=endpoints,
                             ca_cert=ca_cert, cert_key=cert_key, cert_cert=cert_cert, timeout=timeout,
                             user=user, password=password, grpc_options=grpc_options, failover=failover)

    def put(self, key, value, lease=None, prev_kv=False):
        while True:
            try:
                print(self.client.put(key, value))
                break
            except exceptions.Etcd3Exception:
                print("Connection failed trying different endpoint")

    def get(self, key):
        while True:
            try:
                return self.client.get(key)
                break
            except Etcd3Exception:
                print("Connection failed trying different endpoint")


def main():
    first_endpoint = Endpoint(host="localhost", port="2379", secure=False)
    second_endpoint = Endpoint(host="localhost", port="2378", secure=False)
    third_endpoint = Endpoint(host="localhost", port="2377", secure=False)
    endpoints = {"first": first_endpoint, "second": second_endpoint, "third": third_endpoint}
    client_facade = ClientFacade(failover=True, endpoints=endpoints)
    client_facade.put("Key", "Value")


if __name__ == '__main__':
    main()
