VERSION = (2, 0, 0)


def get_version(positions=None):
    version = VERSION
    if positions and isinstance(positions, int):
        version = VERSION[:positions]
    version = (str(v) for v in version)
    return '.'.join(version)
