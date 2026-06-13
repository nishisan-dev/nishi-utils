"""Exceptions raised by the ngrrd Python reader."""


class NgrrdFormatError(ValueError):
    """Raised when an NGRR file is truncated, corrupted, or unsupported."""
