from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime

import Utility.DBConnector as Connector
from Utility.DBConnector import ResultSet

from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# ---------------------------------- CRUD API: ----------------------------------

def create_tables():
    
    queries = [
        """
        CREATE TABLE IF NOT EXISTS Owners(
            id INTEGER PRIMARY KEY check(id > 0),
            name TEXT NOT NULL
          
        )
        """,            
        """
        CREATE TABLE IF NOT EXISTS Customers(
            id INTEGER PRIMARY KEY check(id > 0),
            name TEXT NOT NULL
            
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Apartments(
            id INTEGER PRIMARY KEY check(id > 0),
            address TEXT NOT NULL,
            city TEXT NOT NULL,
            country TEXT NOT NULL,
            size int NOT NULL check(size > 0),
            CONSTRAINT unique_apartment UNIQUE(address, city, country)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Reservations(
            customer_id INTEGER NOT NULL check(customer_id > 0),
            apartment_id INTEGER NOT NULL check(apartment_id > 0),
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            total_price FLOAT NOT NULL check(total_price > 0),
            PRIMARY KEY(customer_id, apartment_id, start_date),
            FOREIGN KEY(customer_id) REFERENCES Customers(id) ON DELETE CASCADE,
            FOREIGN KEY(apartment_id) REFERENCES Apartments(id) ON DELETE CASCADE,
            check(start_date < end_date)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS OwnsApartment(
            owner_id INTEGER NOT NULL check(owner_id > 0),
            apartment_id INTEGER NOT NULL check(apartment_id > 0),
            PRIMARY KEY(apartment_id),
            FOREIGN KEY(owner_id) REFERENCES Owners(id) ON DELETE CASCADE,
            FOREIGN KEY(apartment_id) REFERENCES Apartments(id) ON DELETE CASCADE            
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Reviews(
            customer_id INTEGER NOT NULL check(customer_id > 0),
            apartment_id INTEGER NOT NULL check(apartment_id > 0),
            review_date DATE NOT NULL,
            rating INTEGER NOT NULL check(rating >= 1 and rating <= 10),
            review_text TEXT NOT NULL,
            PRIMARY KEY(customer_id, apartment_id),
            FOREIGN KEY(customer_id) REFERENCES Customers(id) ON DELETE CASCADE,
            FOREIGN KEY(apartment_id) REFERENCES Apartments(id) ON DELETE CASCADE            
        )
        """,
        """
        CREATE OR REPLACE VIEW ApartmentRating AS
        SELECT apartment_id, AVG(rating) AS rating
        FROM Reviews
        GROUP BY apartment_id
        """,       
        """
        CREATE OR REPLACE VIEW OwnerRating AS
        SELECT owner_id, AVG(ApartmentRating.rating) AS rating
        FROM ApartmentRating JOIN OwnsApartment ON ApartmentRating.apartment_id = OwnsApartment.apartment_id
        WHERE OwnsApartment.owner_id = owner_id
        GROUP BY owner_id
        """, 
        """
        CREATE OR REPLACE VIEW OwnersAndApartments AS
        SELECT Owners.id as oId, Owners.name as oName, Apartments.id as aId, Apartments.address, Apartments.city, Apartments.country, Apartments.size 
        FROM Owners
        JOIN OwnsApartment ON Owners.id = OwnsApartment.owner_id 
        JOIN Apartments ON OwnsApartment.apartment_id = Apartments.id
        """,
        """
        CREATE OR REPLACE VIEW ApartmentsAndReviews AS
        SELECT * FROM Apartments
        JOIN Reviews ON Apartments.id = Reviews.apartment_id
        """,
        ]
    conn = Connector.DBConnector()
    try:
        for query in queries:
            conn.execute(query)

    except Exception as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def clear_tables():
    conn = Connector.DBConnector()
    try:
        conn.execute("DELETE FROM Owners")
        conn.execute("DELETE FROM Customers")
        conn.execute("DELETE FROM Apartments")
        conn.execute("DELETE FROM Reservations")
        conn.execute("DELETE FROM OwnsApartment")
        conn.execute("DELETE FROM Reviews")
        
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        conn.close()


def drop_tables():
    conn = Connector.DBConnector()
    try:
        conn.execute("DROP TABLE IF EXISTS Owners CASCADE")
        conn.execute("DROP TABLE IF EXISTS Customers CASCADE")
        conn.execute("DROP TABLE IF EXISTS Apartments CASCADE")
        conn.execute("DROP TABLE IF EXISTS Reservations CASCADE")
        conn.execute("DROP TABLE IF EXISTS OwnsApartment CASCADE")
        conn.execute("DROP TABLE IF EXISTS Reviews CASCADE")
        conn.execute("DROP VIEW IF EXISTS ApartmentRating CASCADE")
        conn.execute("DROP VIEW IF EXISTS OwnerRating CASCADE")
        
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        conn.close()


def add_owner(owner: Owner) -> ReturnValue:
    if(owner.get_owner_id() is None or owner.get_owner_id() <= 0): return ReturnValue.BAD_PARAMS
    if(owner.get_owner_name() is None): return ReturnValue.BAD_PARAMS
    conn = Connector.DBConnector()
    try:
        query = sql.SQL("INSERT INTO Owners(id, name) VALUES({id}, {name})").format(
            id=sql.Literal(owner.get_owner_id()),
            name=sql.Literal(owner.get_owner_name())
        )
        rows_effected, _ = conn.execute(query)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except Exception  as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    
    return ReturnValue.OK


def get_owner(owner_id: int) -> Owner:
    conn = Connector.DBConnector()
    resultSet = ResultSet()
    try:
        query = sql.SQL("SELECT * FROM Owners WHERE id={0}").format(sql.Literal(owner_id))
        rows_effected, resultSet = conn.execute(query)
        if rows_effected == 0:
            return Owner.bad_owner()
        if rows_effected > 1:
            print("Error: multiple owners with the same id")
            return Owner.bad_owner()
        
        return Owner(owner_id= resultSet.rows[0][0], owner_name= resultSet.rows[0][1])
    except Exception as e:
        print(e)
        conn.rollback()
        return Owner.bad_owner()
    finally:
        conn.close()
    
def delete_generic(id: int, table: str)->ReturnValue:
    if(id is None or id <= 0): return ReturnValue.BAD_PARAMS
    conn = Connector.DBConnector()
    try:
        query = sql.SQL("DELETE FROM " + table +" WHERE id="+str(id))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def delete_owner(owner_id: int) -> ReturnValue:
    return delete_generic(owner_id, "Owners")

def add_apartment(apartment: Apartment) -> ReturnValue:
    if(apartment.get_id() is None or apartment.get_id() <= 0): return ReturnValue.BAD_PARAMS
    conn = Connector.DBConnector()
    try:
        query = sql.SQL("INSERT INTO Apartments(id, address, city, country, size) VALUES({id}, {addr}, {city}, {country}, {size})").format(
            id=sql.Literal(apartment.get_id()),
            addr=sql.Literal(apartment.get_address()),
            city=sql.Literal(apartment.get_city()),
            country=sql.Literal(apartment.get_country()),
            size=sql.Literal(apartment.get_size())
        )
        rows_effected, _ = conn.execute(query)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except Exception  as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK

def get_apartment(apartment_id: int) -> Apartment:
    conn = Connector.DBConnector()
    try:
        query = sql.SQL("SELECT * FROM Apartments WHERE id={0}").format(sql.Literal(apartment_id))
        rows_effected, resultSet = conn.execute(query)
        if rows_effected == 0:
            return Apartment.bad_apartment()
        if rows_effected > 1:
            print("Error: multiple customers with the same id")
            return Apartment.bad_apartment()
        
        return Apartment(id= resultSet.rows[0][0],
                         address= resultSet.rows[0][1], 
                         city= resultSet.rows[0][2], 
                         country= resultSet.rows[0][3], 
                         size= resultSet.rows[0][4])
    except Exception as e:
        print(e)
        conn.rollback()
        return Apartment.bad_apartment()
    finally:
        conn.close()


def delete_apartment(apartment_id: int) -> ReturnValue:
    return delete_generic(apartment_id, "Apartments")


def add_customer(customer: Customer) -> ReturnValue:
    if(customer.get_customer_id() is None or customer.get_customer_id() <= 0): return ReturnValue.BAD_PARAMS
    if(customer.get_customer_name() is None): return ReturnValue.BAD_PARAMS
    conn = Connector.DBConnector()
    try:
        query = sql.SQL("INSERT INTO Customers(id, name) VALUES({id}, {name})").format(
            id=sql.Literal(customer.get_customer_id()),
            name=sql.Literal(customer.get_customer_name())
        )
        rows_effected, _ = conn.execute(query)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except Exception  as e: 
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    
    finally:
        conn.close()

    return ReturnValue.OK


def get_customer(customer_id: int) -> Customer:
    conn = Connector.DBConnector()
    try:
        query = sql.SQL("SELECT * FROM Customers WHERE id={0}").format(sql.Literal(customer_id))
        rows_effected, resultSet = conn.execute(query)
        if rows_effected == 0:
            return Customer.bad_customer()
        if rows_effected > 1:
            print("Error: multiple customers with the same id")
            return Customer.bad_customer()
        
        return Customer(customer_id= resultSet.rows[0][0], customer_name= resultSet.rows[0][1])
    except Exception as e:
        print(e)
        conn.rollback()
        return Customer.bad_customer()
    finally:
        conn.close()        

def delete_customer(customer_id: int) -> ReturnValue:
    return delete_generic(customer_id, "Customers")


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date, total_price: float) -> ReturnValue:

    conn = Connector.DBConnector()
    try:
        query = sql.SQL("""
                        INSERT INTO Reservations(
                            customer_id, apartment_id, start_date, end_date, total_price) 
                            SELECT {cid}, {aid}, {sd}, {ed}, {tp}
                            WHERE NOT EXISTS (
                                SELECT * FROM Reservations 
                                WHERE apartment_id = {aid} AND (
                                    {sd} BETWEEN start_date AND end_date OR
                                    {ed} BETWEEN start_date AND end_date OR
                                    (start_date  > {sd} AND end_date < {ed})
                                )
                            )
                        """).format(cid=sql.Literal(customer_id),
                        aid=sql.Literal(apartment_id),
                        sd=sql.Literal(start_date),
                        ed=sql.Literal(end_date),
                        tp=sql.Literal(total_price)
        )
        rows_effected, _ = conn.execute(query)
        if(rows_effected == 0):
            return ReturnValue.BAD_PARAMS
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION) as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print("Reservation already exists, shouldn't be possible")
        return ReturnValue.ALREADY_EXISTS
    except Exception  as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK
        


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    if(customer_id is None or customer_id <= 0 or apartment_id is None or apartment_id <= 0 or start_date is None): return ReturnValue.BAD_PARAMS
    conn = Connector.DBConnector()
    try:
        query = sql.SQL("DELETE FROM Reservations WHERE customer_id={cid} AND apartment_id={aid} AND start_date={sd}").format(
            cid=sql.Literal(customer_id),
            aid=sql.Literal(apartment_id),
            sd=sql.Literal(start_date)
        )
        rows_effected, _ = conn.execute(query)
        if(rows_effected == 0):
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int, review_text: str) -> ReturnValue:
    conn = Connector.DBConnector()
    try:
        queryStr = """
            INSERT INTO Reviews(
                SELECT {cid}, {aid}, {rd}, {r}, {rt}
                WHERE EXISTS (
                    SELECT * FROM Reservations 
                    WHERE end_date < {rd} AND customer_id = {cid} AND apartment_id = {aid}                )
            );
        """
        query = sql.SQL(queryStr).format(
            cid=sql.Literal(customer_id),
            aid=sql.Literal(apartment_id),
            rd=sql.Literal(review_date),
            r=sql.Literal(rating),
            rt=sql.Literal(review_text)
        )
        rows_effected, _ = conn.execute(query)
        if(rows_effected == 0):
            return ReturnValue.NOT_EXISTS
        
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION) as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except Exception  as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK
        

def customer_updated_review(customer_id: int, apartment_id: int, update_date: date, new_rating: int, new_text: str) -> ReturnValue:
    conn = Connector.DBConnector()
    try:
        query = sql.SQL("UPDATE Reviews SET review_date={ud}, rating={nr}, review_text={nt} WHERE customer_id={cid} AND apartment_id={aid} AND review_date < {ud}").format(
            ud=sql.Literal(update_date),
            nr=sql.Literal(new_rating),
            nt=sql.Literal(new_text),
            cid=sql.Literal(customer_id),
            aid=sql.Literal(apartment_id)
        )
        rows_effected, res = conn.execute(query)
        if(rows_effected == 0):
            return ReturnValue.NOT_EXISTS

    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION) as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.NOT_EXISTS
    except Exception  as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK
        

def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    conn = Connector.DBConnector()
    try:
        query = sql.SQL("INSERT INTO OwnsApartment(owner_id, apartment_id) VALUES({oid}, {aid})").format(
            oid=sql.Literal(owner_id),
            aid=sql.Literal(apartment_id)
        )
        rows_effected, _ = conn.execute(query)
    
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION) as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except Exception  as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    
    return ReturnValue.OK


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    if(owner_id is None or owner_id <= 0 or apartment_id is None or apartment_id <= 0): return ReturnValue.BAD_PARAMS
    conn = Connector.DBConnector()
    try:
        query = sql.SQL("DELETE FROM OwnsApartment WHERE owner_id={oid} AND apartment_id={aid}").format(
            oid=sql.Literal(owner_id),
            aid=sql.Literal(apartment_id)
        )
        rows_effected, _ = conn.execute(query)
        if(rows_effected == 0):
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


#TODO: check manually
def get_apartment_owner(apartment_id: int) -> Owner:
    conn = Connector.DBConnector()
    resultSet = ResultSet()
    try:
        query = sql.SQL("""
            SELECT Owners.id, Owners.name
            FROM Owners
            WHERE(Owners.id = (SELECT owner_id FROM OwnsApartment WHERE apartment_id = {oid}))
        """).format(oid = sql.Literal(apartment_id))
        rows_effected, resultSet = conn.execute(query)
        if(resultSet.isEmpty()): return Owner.bad_owner()
        return Owner(owner_id= resultSet.rows[0][0], owner_name= resultSet.rows[0][1])
    except Exception as e:
        print(e)
        conn.rollback()
        return Owner.bad_owner()
    finally:
        conn.close()

#TODO: check manually
def get_owner_apartments(owner_id: int) -> List[Apartment]:
    conn = Connector.DBConnector()
    resultSet = ResultSet()
    try:
        query = sql.SQL("""
            SELECT Apartments.id, Apartments.address, Apartments.city, Apartments.country, Apartments.size
            FROM Apartments
            JOIN OwnsApartment ON Apartments.id = OwnsApartment.apartment_id
            WHERE(OwnsApartment.owner_id = {oid})
        """).format(oid = sql.Literal(owner_id))
        rows_effected, resultSet = conn.execute(query)
        if(resultSet.isEmpty()): return []
        apartments = []
        for row in resultSet.rows:
            apartments.append(Apartment(id= row[0], address= row[1], city= row[2], country= row[3], size= row[4]))
        return apartments
    except Exception as e:
        print(e)
        conn.rollback()
        return []
    finally:
        conn.close()


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    conn = Connector.DBConnector()
    resultSet = ResultSet()
    try:
        query = sql.SQL("SELECT rating FROM ApartmentRating WHERE apartment_id={0}").format(sql.Literal(apartment_id))
        rows_effected, resultSet = conn.execute(query)
        if(resultSet.isEmpty()): return 0.0
        return resultSet.rows[0][0]
    except Exception as e:
        print(e)
        conn.rollback()
        return 0.0
    finally:
        conn.close()


def get_owner_rating(owner_id: int) -> float:
    conn = Connector.DBConnector()
    resultSet = ResultSet()
    try:
        query = sql.SQL("SELECT rating FROM OwnerRating WHERE owner_id={0}").format(sql.Literal(owner_id))
        rows_effected, resultSet = conn.execute(query)
        if(resultSet.isEmpty()): return 0.0
        return resultSet.rows[0][0]
    except Exception as e:
        print(e)
        conn.rollback()
        return 0.0
    finally:
        conn.close()


def get_top_customer() -> Customer:
    conn = Connector.DBConnector()
    resultSet = ResultSet()
    try:
        query = sql.SQL("""
            SELECT Customers.id, Customers.name
            FROM Customers
            JOIN Reservations ON Customers.id = Reservations.customer_id
            GROUP BY Customers.id
            ORDER BY COUNT(*) DESC, Customers.id ASC
            LIMIT 1
        """)
        rows_effected, resultSet = conn.execute(query)
        if(resultSet.isEmpty()): return Customer.bad_customer()
        return Customer(customer_id= resultSet.rows[0][0], customer_name= resultSet.rows[0][1])
    except Exception as e:
        print(e)
        conn.rollback()
        return Customer.bad_customer()
    finally:
        conn.close()


def reservations_per_owner() -> List[Tuple[str, int]]:
    conn = Connector.DBConnector()
    resultSet = ResultSet()
    try:
        query = sql.SQL("""
            SELECT Owners.name as owner_name, COUNT(*) as total_reservation_count
            FROM OwnersAndApartments
            JOIN Reservations ON Apartments.id = Reservations.apartment_id 
            GROUP BY Owners.id, Owners.name
        """)
        rows_effected, resultSet = conn.execute(query)
        if(resultSet.isEmpty()): return []
        owners = []
        for row in resultSet.rows:
            owners.append((row[0], row[1]))
        return owners
    except Exception as e:
        print(e)
        conn.rollback()
        return []
    finally:
        conn.close()


# ---------------------------------- ADVANCED API: ----------------------------------

#todo: validate if Apartments.city should have been OwnersAndApartments.city
def get_all_location_owners() -> List[Owner]:
    conn = Connector.DBConnector()
    resultSet = ResultSet()
    try:
        query = sql.SQL("""
            SELECT Owners.id, Owners.name
            FROM OwnersAndApartments
            GROUP BY Owners.id, Owners.name
            WHERE COUNT (DISTINCT Apartments.city) = (SELECT COUNT(DISTINCT Apartments.city) FROM Apartments)
        """)
        rows_effected, resultSet = conn.execute(query)
        if(resultSet.isEmpty()): return []
        owners = []
        for row in resultSet.rows:
            owners.append(Owner(owner_id= row[0], owner_name= row[1]))
        return owners
    except Exception as e:
        print(e)
        conn.rollback()
        return []
    finally:
        conn.close()


def best_value_for_money() -> Apartment:
    conn = Connector.DBConnector()
    resultSet = ResultSet()
    try:
        query = sql.SQL("""
            SELECT ApartmentsAndReviews.id, ApartmentsAndReviews.address, ApartmentsAndReviews.city, ApartmentsAndReviews.country, ApartmentsAndReviews.size
            FROM ApartmentsAndReviews
            JOIN Reservations ON ApartmentsAndReviews.id = Reservations.apartment_id
            GROUP BY ApartmentsAndReviews.id, ApartmentsAndReviews.address, ApartmentsAndReviews.city, ApartmentsAndReviews.country, ApartmentsAndReviews.size
            ORDER BY AVG(ApartmentsAndReviews.rating) / 
            AVG(Reservations.total_price / DATEDIFF(d, Reservations.end_date, Reservations.start_date))
            ASC
            LIMIT 1
        """)
        rows_effected, resultSet = conn.execute(query)
        if(resultSet.isEmpty()): return Apartment.bad_apartment()
        return Apartment(id= resultSet.rows[0][0], address= resultSet.rows[0][1], city= resultSet.rows[0][2], country= resultSet.rows[0][3], size= resultSet.rows[0][4])
    except Exception as e:
        print(e)
        conn.rollback()
        return Apartment.bad_apartment()
    finally:
        conn.close()


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    conn = Connector.DBConnector()
    resultSet = ResultSet()
    try:
        allMonth = "DECLARE @allMonth TABLE (month INT);"
        allMonth += "INSERT INTO @allMonth VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);"
        conn.execute(allMonth)
        
        query = sql.SQL("""
            SELECT @allMonth.month, IFNULL(total_profit, 0) FROM @allMonth 
            LEFT JOIN (
                SELECT EXTRACT(MONTH FROM end_date) as month, SUM(total_price) * 0.15 as total_profit
                FROM Reservations
                WHERE EXTRACT(YEAR FROM end_date) = {year}
                GROUP BY EXTRACT(MONTH FROM end_date)
                ORDER BY EXTRACT(MONTH FROM end_date) ASC
            )
        """).format(year=sql.Literal(year))
        rows_effected, resultSet = conn.execute(query)
        if(resultSet.isEmpty()): return []
        profits = []
        for row in resultSet.rows:
            profits.append((row[0], row[1]))
        return profits
    except Exception as e:
        print(e)
        conn.rollback()
        return []
    finally:
        conn.close()


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    conn = Connector.DBConnector()
    resultSet = ResultSet()
    try:
        query = sql.SQL("""
            WITH ReviewTuples AS (
                SELECT ApartmentsAndReviews.id as ThisCustId, ApartmentsAndReviews.rating as ThisCustRating, 
                       Reviews.id as OtherCustId, Reviews.rating as OtherCustRating,
                       Appartments.id as AppId
                FROM ApartmentsAndReviews
                JOIN Reviews ON ApartmentsAndReviews.id = Reviews.apartment_id
                WHERE ApartmentsAndReviews.customer_id = {cid} AND Reviews.customer_id <> {cid}
                )
                
            WITH Mitam as (
                SELECT ReviewTuples.OtherCustId, AVG(ReviewTuples.OtherCustRating / ReviewTuples.ThisCustRating) 
                FROM ReviewTuples
                GROUP BY ReviewTuples.OtherCustId 
            )
            
            SELECT Apartments.id, Apartments.address, Apartments.city, Apartments.country, Apartments.size, AVG(Mitam.rating)
            FROM  ApartmentsAndReviews
            JOIN Mitam ON ApartmentsAndReviews.customer_id = Mitam.OtherCustId
            WHERE NOT EXISTS (
                SELECT * FROM Reviews 
                WHERE apartment_id = ApartmentsAndReviews.id AND customer_id = {cid}
            )
            GROUP BY Apartments.id, Apartments.address, Apartments.city, Apartments.country, Apartments.size
        """).format(cid=sql.Literal(customer_id))
        rows_effected, resultSet = conn.execute(query)
        if(resultSet.isEmpty()): return []
        apartments = []
        for row in resultSet.rows:
            apartments.append((Apartment(id= row[0], address= row[1], city= row[2], country= row[3], size= row[4]), row[5]))
        return apartments
    except Exception as e:
        print(e)
        conn.rollback()
        return []
    finally:
        conn.close()