def prefix_range_end(prefix):
    """Create a bytestring that can be used as a range_end for a prefix."""
    s = bytearray(prefix)
    for i in reversed(range(len(s))):
        if s[i] < 0xff:
            s[i] = s[i] + 1
            break
    return bytes(s)


def to_bytes(maybe_bytestring):
    """
    Encode string to bytes.

    Convenience function to do a simple encode('utf-8') if the input is not
    already bytes. Returns the data unmodified if the input is bytes.
    """
    if isinstance(maybe_bytestring, bytes):
        return maybe_bytestring
    else:
        return maybe_bytestring.encode('utf-8')


def lease_to_id(lease):
    """Figure out if the argument is a Lease object, or the lease ID."""
    lease_id = 0
    if hasattr(lease, 'id'):
        lease_id = lease.id
    else:
        try:
            lease_id = int(lease)
        except TypeError:
            pass
    return lease_id


def response_to_event_iterator(response_iterator):
    """Convert a watch response iterator to an event iterator."""
    for response in response_iterator:
        for event in response.events:
            yield event
