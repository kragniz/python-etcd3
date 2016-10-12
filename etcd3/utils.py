def increment_last_byte(byte_string):
    return byte_string[:-1] + bytes([byte_string[-1] + 1])
