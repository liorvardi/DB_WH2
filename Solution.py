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
    conn = Connector.DBConnector()
    try:
        queries = [
            """
            CREATE TABLE Owners(
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
            """,            
            """
            CREATE TABLE Customers(
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE Apartments(
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
            CREATE TABLE Reservations(
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
            CREATE TABLE OwnsApartment(
                owner_id INTEGER NOT NULL,
                apartment_id INTEGER NOT NULL,
                PRIMARY KEY(apartment_id),
                FOREIGN KEY(owner_id) REFERENCES Owners(id) ON DELETE CASCADE,
                FOREIGN KEY(apartment_id) REFERENCES Apartments(id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE Reviews(
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
            """
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
    # TODO: implement
    pass


def drop_tables():
    # TODO: implement
    pass


def add_owner(owner: Owner) -> ReturnValue:
    # TODO: implement
    pass


def get_owner(owner_id: int) -> Owner:
    # TODO: implement
    pass


def delete_owner(owner_id: int) -> ReturnValue:
    # TODO: implement
    pass


def add_apartment(apartment: Apartment) -> ReturnValue:
    if(apartment.get_id() is None or apartment.get_id() <= 0
       or apartment.get_address is None or ): return ReturnValue.BAD_PARAMS
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
    # TODO: implement
    pass


def delete_apartment(apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


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
    resultSet = ResultSet()
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
    if(customer_id is None or customer_id <= 0): return ReturnValue.BAD_PARAMS
    conn = Connector.DBConnector()
    try:
        query = sql.SQL("DELETE FROM Customers WHERE id={0}").format(sql.Literal(customer_id))
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


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date, total_price: float) -> ReturnValue:
    # TODO: implement
    pass


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    # TODO: implement
    pass


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int, review_text: str) -> ReturnValue:
    # TODO: implement
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
