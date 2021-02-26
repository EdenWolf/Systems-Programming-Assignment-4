import atexit
import sqlite3

from DAO import _Vaccines, _Suppliers, _Clinics, _Logistics
from DTO import Vaccine, Supplier, Clinic, Logistic


class _Repository:
    def __init__(self, inputfilename, ordersfilename, outputfilename):
        self._conn = sqlite3.connect('database.db')
        self.vaccines = _Vaccines(self._conn)
        self.suppliers = _Suppliers(self._conn)
        self.clinics = _Clinics(self._conn)
        self.logistics = _Logistics(self._conn)
        self.create_tables()
        self.configure(inputfilename)
        self.order(ordersfilename, outputfilename)

    def _close(self):
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        self._conn.executescript("""
        CREATE TABLE vaccines (
            id INTEGER PRIMARY KEY,
            date DATE NOT NULL,
            supplier INTEGER REFERENCES Supplier(id),
            quantity INTEGER NOT NULL
        );
        
        CREATE TABLE suppliers (
            id INTEGER PRIMARY KEY,
            name STRING NOT NULL,
            logistic INTEGER REFERENCES Logistic(id)
        );
        
        CREATE TABLE clinics (
            id INTEGER PRIMARY KEY,
            location STRING NOT NULL,
            demand INTEGER NOT NULL,
            logistic INTEGER REFERENCES Logistic(id)
        );
        
        CREATE TABLE logistics (
            id INTEGER PRIMARY KEY,
            name STRING NOT NULL,
            count_sent INTEGER NOT NULL,
            count_received INTEGER NOT NULL
        );
        """)

    def configure(self, inputfilename):
        with open(inputfilename) as inputfile:
            index = inputfile.readline().split(',')
            numOfVaccines = int(index[0])
            numOfSuppliers = int(index[1])
            numOfClinics = int(index[2])
            numOfLogistics = int(index[3])
            while numOfVaccines != 0:  # vaccines
                line = inputfile.readline().split(',')
                self.vaccines.insert(Vaccine(line[0], line[1], line[2], line[3]))
                numOfVaccines = numOfVaccines - 1
            while numOfSuppliers != 0:  # suppliers
                line = inputfile.readline().split(',')
                self.suppliers.insert(Supplier(line[0], line[1], line[2]))
                numOfSuppliers = numOfSuppliers - 1
            while numOfClinics != 0:  # clinics
                line = inputfile.readline().split(',')
                self.clinics.insert(Clinic(line[0], line[1], line[2], line[3]))
                numOfClinics = numOfClinics - 1
            while numOfLogistics != 0:  # logistics
                line = inputfile.readline().split(',')
                self.logistics.insert(Logistic(line[0], line[1], line[2], line[3]))
                numOfLogistics = numOfLogistics - 1

    def order(self, ordersfilename, outputfilename):
        with open(ordersfilename) as orderfile, open(outputfilename, 'w') as outputFile:
            for line in orderfile:
                order = line.split(',')
                if len(order) == 3:  # receive shipment
                    outputFile.write(self.receiveShipment(order))
                else:  # send shipment
                    outputFile.write(self.sendShipment(order))

    def receiveShipment(self, order):
        name = order[0]  # supplier name
        amount = order[1]
        date = order[2]
        newVaccineId = self.getNextVaccineId()
        supplierId = self.getSupplierId(name)
        self.addVaccine(newVaccineId, date, supplierId, amount)
        self.addToReceived(name, amount)
        return self.getOutput()

    def sendShipment(self, order):
        location = order[0]
        amount = order[1]
        self.updateDemand(int(amount), location)
        self.removeVaccines(int(amount))
        logisticId = self.getLogisticIdFromClinic(location)
        self.updateCountSent(logisticId, int(amount))
        return self.getOutput()

# get the next vaccine id we will use
    def getNextVaccineId(self):
        cursor = self._conn.cursor()
        cursor.execute("""
            SELECT MAX(id) FROM vaccines;
        """)
        return int(cursor.fetchone()[0]) + 1

# get the supplier id from it's name
    def getSupplierId(self, name):
        cursor = self._conn.cursor()
        cursor.execute("""
            SELECT id FROM suppliers WHERE name LIKE (?);
        """, (name,))
        return int(cursor.fetchone()[0])

# add a vaccine to the vaccines table
    def addVaccine(self, id, date, supplierId, amount):
        cursor = self._conn.cursor()
        cursor.execute("""
            INSERT into vaccines VALUES (?, ?, ?, ?)
        """, (id, date, supplierId, amount))

# update the count_received
    def addToReceived(self, name, amount):
        logisticId = self.getLogisticIdFromSupplier(name)
        self.updateCountReceived(logisticId, int(amount))

# get the logistic id from the supplier
    def getLogisticIdFromSupplier(self, name):
        cursor = self._conn.cursor()
        cursor.execute("""
            SELECT logistic FROM suppliers WHERE name LIKE (?)
        """, (name,))
        return cursor.fetchone()[0]

# update the count_received in the logistics table
    def updateCountReceived(self, logisticId, intAmount):
        cursor = self._conn.cursor()
        cursor.execute("""
            SELECT count_received FROM logistics WHERE id LIKE (?)
        """, (logisticId,))
        count = int(cursor.fetchone()[0]) + intAmount
        cursor.execute("""
            UPDATE logistics set count_received=(?) WHERE id LIKE (?)
        """, (count, logisticId))

# new line for output file - receive
    def getOutput(self):
        cursor = self._conn.cursor()
        cursor.execute("""
            SELECT SUM(quantity) FROM vaccines;
        """)
        total_inventory = cursor.fetchone()[0]
        cursor.execute("""
            SELECT SUM(demand) FROM clinics;
        """)
        total_demand = cursor.fetchone()[0]
        cursor.execute("""
            SELECT SUM(count_received) FROM logistics;
        """)
        total_received = cursor.fetchone()[0]
        cursor.execute("""
            SELECT SUM(count_sent) FROM logistics;
        """)
        total_sent = cursor.fetchone()[0]
        return str(total_inventory) + ',' + str(total_demand) + ',' + str(total_received) + ',' + str(total_sent) + '\n'

# update location demand
    def updateDemand(self, intAmount, location):
        cursor = self._conn.cursor()
        cursor.execute("""
            SELECT demand FROM clinics WHERE location LIKE (?)
        """, (location,))
        prevDemand = int(cursor.fetchone()[0])
        newDemand = prevDemand - intAmount
        cursor.execute("""
            UPDATE clinics set demand=(?) WHERE location LIKE (?)
        """, (str(newDemand), location))

# remove vaccines from inventory
    def removeVaccines(self, intAmount):
        while intAmount > 0:
            cursor = self._conn.cursor()
            cursor.execute("""
                SELECT MIN (date) FROM vaccines;
            """)
            date = cursor.fetchone()[0]
            cursor.execute("""
                SELECT quantity FROM vaccines WHERE date LIKE (?);
            """, (date,))
            quantity = int(cursor.fetchone()[0])
            if intAmount >= quantity:
                cursor.execute("""
                    DELETE FROM vaccines WHERE date LIKE (?);
                """, (date,))
            else:
                newQuantity = quantity - intAmount
                cursor.execute("""
                    UPDATE vaccines set quantity=(?) WHERE date LIKE (?);
                """, (str(newQuantity), date))
            intAmount -= quantity

# update the count_sent in the logistics table
    def updateCountSent(self, logisticId, intAmount):
        cursor = self._conn.cursor()
        cursor.execute("""
            SELECT count_sent FROM logistics WHERE id LIKE (?)
        """, (logisticId,))
        count = int(cursor.fetchone()[0]) + intAmount
        cursor.execute("""
            UPDATE logistics set count_sent=(?) WHERE id LIKE (?)
        """, (count, logisticId))

# get the logistic id from the clinic
    def getLogisticIdFromClinic(self, location):
        cursor = self._conn.cursor()
        cursor.execute("""
            SELECT logistic FROM clinics WHERE location LIKE (?)
        """, (location,))
        return cursor.fetchone()[0]