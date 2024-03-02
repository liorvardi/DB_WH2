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
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
        """,            
        """
        CREATE TABLE IF NOT EXISTS Customers(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Apartments(
            id INTEGER PRIMARY KEY,
            address TEXT NOT NULL,
            city TEXT NOT NULL,
            country TEXT NOT NULL,
            size int NOT NULL,
            check(size > 0),
            CONSTRAINT unique_apartment UNIQUE(address, city, country)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Reservations(
            customer_id INTEGER NOT NULL,
            apartment_id INTEGER NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            total_price FLOAT NOT NULL,
            PRIMARY KEY(customer_id, apartment_id, start_date),
            FOREIGN KEY(customer_id) REFERENCES Customers(id) ON DELETE CASCADE,
            FOREIGN KEY(apartment_id) REFERENCES Apartments(id) ON DELETE CASCADE,
            check(start_date < end_date),
            check(total_price > 0)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS OwnsApartment(
            owner_id INTEGER NOT NULL,
            apartment_id INTEGER NOT NULL,
            PRIMARY KEY(apartment_id),
            FOREIGN KEY(owner_id) REFERENCES Owners(id) ON DELETE CASCADE,
            FOREIGN KEY(apartment_id) REFERENCES Apartments(id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Reviews(
            customer_id INTEGER NOT NULL,
            apartment_id INTEGER NOT NULL,
            review_date DATE NOT NULL,
            rating INTEGER NOT NULL,
            review_text TEXT NOT NULL,
            PRIMARY KEY(customer_id, apartment_id),
            FOREIGN KEY(customer_id) REFERENCES Customers(id) ON DELETE CASCADE,
            FOREIGN KEY(apartment_id) REFERENCES Apartments(id) ON DELETE CASCADE,
            check(rating >= 1 and rating <= 10)
        )
        """,
        """
        CREATE VIEW ApartmentRating AS
        SELECT apartment_id, AVG(rating) AS rating
        FROM Reviews
        GROUP BY apartment_id
        """,       
        """
        CREATE VIEW OwnerRating AS
        SELECT owner_id, AVG(rating) AS rating
        FROM Reviews
        JOIN OwnsApartment ON Reviews.apartment_id = OwnsApartment.apartment_id
        GROUP BY owner_id
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
    if(owner.get_id() is None or owner.get_id() <= 0): return ReturnValue.BAD_PARAMS
    if(owner.get_name() is None): return ReturnValue.BAD_PARAMS
    conn = Connector.DBConnector()
    try:
        query = sql.SQL("INSERT INTO Owners(id, name) VALUES({id}, {name})").format(
            id=sql.Literal(owner.get_id()),
            name=sql.Literal(owner.get_name())
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
        query = sql.SQL("DELETE FROM {table} WHERE id={0}").format(sql.Literal(table), sql.Literal(id))
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
        query = sql.SQL("INSERT INTO Apartments(id, owner_id, name, location, price) VALUES({id}, {addr}, {city}, {county}, {size})").format(
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
        query = sql.SQL("SELECT * FROM Apartament WHERE id={0}").format(sql.Literal(apartment_id))
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
                            VALUES({cid}, {aid}, {sd}, {ed}, {tp})
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


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int, review_text: str) -> ReturnValue:
    if(customer_id is None or customer_id <= 0 or apartment_id is None or apartment_id <= 0 or review_date is None or rating is None or rating < 1 or rating > 10 or review_text is None): return ReturnValue.BAD_PARAMS
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
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION) as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    pass


def customer_updated_review(customer_id: int, apartmetn_id: int, update_date: date, new_rating: int, new_text: str) -> ReturnValue:
    # TODO: implement
    pass


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def get_apartment_owner(apartment_id: int) -> Owner:
    # TODO: implement
    pass


def get_owner_apartments(owner_id: int) -> List[Apartment]:
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    # TODO: implement
    pass


def get_owner_rating(owner_id: int) -> float:
    # TODO: implement
    pass


def get_top_customer() -> Customer:
    # TODO: implement
    pass


def reservations_per_owner() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------

def get_all_location_owners() -> List[Owner]:
    # TODO: implement
    pass


def best_value_for_money() -> Apartment:
    # TODO: implement
    pass


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    pass


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement
    pass
