def increment_last_byte(byte_string):
    s = bytearray(byte_string)
    s[-1] = s[-1] + 1
    return bytes(s)


def to_bytes(maybe_bytestring):
    '''
    Convenience function to do a simple encode('utf-8') if the input is not
    already bytes. Returns the data unmodified if the input is bytes.
    '''
    if isinstance(maybe_bytestring, bytes):
        return maybe_bytestring
    else:
        return maybe_bytestring.encode('utf-8')
