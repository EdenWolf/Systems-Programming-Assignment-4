import atexit
import sqlite3
import sys

from DAO import _Vaccines, _Suppliers, _Clinics
from DTO import Vaccine, Supplier, Clinic, Logistic
from Repository import _Repository


def main(args):
    repo = _Repository(args[1], args[2], args[3])
    atexit.register(repo._close)



main(sys.argv)
