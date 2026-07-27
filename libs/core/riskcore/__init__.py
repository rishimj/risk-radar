"""Shared domain logic for RiskRadar.

Imported by every service *and* by the PyFlink job, which is why it is a real
installable package rather than copied source: the old repo kept three divergent
copies of the company database and models.
"""

__version__ = "0.1.0"
