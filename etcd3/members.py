class Member(object):
    def __init__(self, id, name, peer_urls, client_urls, etcd_client=None):
        self.id = id
        self.name = name
        self.peer_urls = peer_urls
        self.client_urls = client_urls
        self.etcd_client = etcd_client

    def __str__(self):
        return ('Member {id}: peer urls: {peer_urls}, client '
                'urls: {client_urls}'.format(id=self.id,
                                             peer_urls=self.peer_urls,
                                             client_urls=self.client_urls))

    def remove(self):
        '''
        Remove this member from the cluster.
        '''
        self.etcd_client.remove_member(self.id)

    def update(self, peer_urls):
        '''
        Update the configuration of this member.

        :param peer_urls: new list of peer urls the member will use to
                          communicate with the cluster
        '''
        self.etcd_client.update_member(self.id, peer_urls)
