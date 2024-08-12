
import pathlib


def validate(path: pathlib.Path, **kwargs) -> bool:
    if 'min_length' in kwargs and path.stat().st_size > kwargs['min_length']:
        return True
    return False

