def increment_last_byte(byte_string):
    s = bytearray(byte_string)
    s[-1] = s[-1] + 1
    return bytes(s)
