class Member(object):
    def __init__(self, id, name, peer_urls, client_urls):
        self.id = id
        self.name = name
        self.peer_urls = peer_urls
        self.client_urls = client_urls

    def __str__(self):
        return ('Member {id}: peer urls: {peer_urls}, client '
                'urls: {client_urls}'.format(id=self.id,
                                             peer_urls=self.peer_urls,
                                             client_urls=self.client_urls))
