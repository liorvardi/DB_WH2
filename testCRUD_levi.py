import unittest
from Solution import *
from Utility.ReturnValue import ReturnValue


class TestCRUD(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        create_tables()

    @classmethod
    def tearDownClass(cls):
        # This method will be called once after all tests are executed
        # Clean up your test environment
        drop_tables()
  

    def test_Owner(self):
        self.assertEqual(add_owner(Owner(1, "Dan")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(2, "Yuval")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(3, "Nadav")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(3, "Nadav")), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(add_owner(Owner(0, "")), ReturnValue.BAD_PARAMS)
        # self.assertEqual(add_owner(None), ReturnValue.ERROR)

        self.assertEqual(get_owner(1).get_owner_id(), 1)

        self.assertEqual(delete_owner(3), ReturnValue.OK)
        self.assertEqual(delete_owner(3), ReturnValue.NOT_EXISTS)
        self.assertEqual(delete_owner(0), ReturnValue.BAD_PARAMS)
        self.assertEqual(delete_owner(None), ReturnValue.BAD_PARAMS)
        # self.assertEqual(delete_owner(None), ReturnValue.ERROR)

        self.assertEqual(get_owner(3), Owner.bad_owner())
        self.assertEqual(add_owner(Owner(3, "Nadav")), ReturnValue.OK)

    def test_apartment(self):
        self.assertEqual(add_apartment(Apartment(1, "123 Main St", "Haifa", "Israel", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(2, "456 Elm St", "NY", "NY", 800)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(3, "123 Main St", "Haifa", "lebanon", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(0, 0, 0, 0, 0)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_apartment(Apartment(1, "123 Main St", "Haifa", "Israel", 80)), ReturnValue.ALREADY_EXISTS)
        # self.assertEqual(add_apartment(None), ReturnValue.ERROR)

        self.assertEqual(get_apartment(1).get_id(), 1)

        self.assertEqual(delete_apartment(3), ReturnValue.OK)
        self.assertEqual(delete_apartment(3), ReturnValue.NOT_EXISTS)
        self.assertEqual(delete_apartment(0), ReturnValue.BAD_PARAMS)
        # self.assertEqual(delete_apartment(None), ReturnValue.ERROR)

        # self.assertEqual(get_apartment(1), Apartment.bad_apartment())
        self.assertEqual(add_apartment(Apartment(3, "123 Main St", "Haifa", "lebanon", 80)), ReturnValue.OK)

    def test_customer(self):
        self.assertEqual(add_customer(Customer(1, "Dani")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(2, "Yuvali")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(3, "Nadavi")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(3, "Nadavi")), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(add_customer(Customer(0, "")), ReturnValue.BAD_PARAMS)
        # self.assertEqual(add_customer(None), ReturnValue.BAD_PARAMS)

        self.assertEqual(get_customer(1).get_customer_id(), 1)

        self.assertEqual(delete_customer(3), ReturnValue.OK)
        self.assertEqual(delete_customer(3), ReturnValue.NOT_EXISTS)
        self.assertEqual(delete_customer(0), ReturnValue.BAD_PARAMS)
        self.assertEqual(delete_customer(None), ReturnValue.BAD_PARAMS)
        # self.assertEqual(delete_customer(None), ReturnValue.ERROR)

        self.assertEqual(get_customer(3), Customer.bad_customer())

    def test_owner_owns_apartment(self):
        self.assertEqual(owner_owns_apartment(1, 1), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(2, 2), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(1, 3), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(1, 1), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(owner_owns_apartment(1, 10), ReturnValue.NOT_EXISTS)
        self.assertEqual(owner_owns_apartment(0, 0), ReturnValue.BAD_PARAMS)

        self.assertEqual(get_apartment_owner(1).get_owner_id(), 1)
        self.assertEqual([o.get_id() for o in get_owner_apartments(1)], [1, 3])

        self.assertEqual(get_apartment_owner(1).get_owner_id(), 1)
        self.assertEqual(get_owner_apartments(3), [])

        self.assertEqual(owner_owns_apartment(1, 3), ReturnValue.ALREADY_EXISTS)

    def test_owner_drops_apartment(self):  
        # Test dropping an existing apartment
        result = owner_drops_apartment(1, 1)
        self.assertEqual(result, ReturnValue.OK)

        # Test dropping a non-existing apartment
        result = owner_drops_apartment(2, 1)
        self.assertEqual(result, ReturnValue.NOT_EXISTS)

        # Test dropping with invalid parameters
        result = owner_drops_apartment(1, 0)
        self.assertEqual(result, ReturnValue.BAD_PARAMS)

        # Test dropping with foreign key violation
        result = owner_drops_apartment(1, 5)
        self.assertEqual(result, ReturnValue.NOT_EXISTS)

        # Test dropping with an error
        result = owner_drops_apartment(1, 1)
        self.assertEqual(result, ReturnValue.ERROR)

    def test_customer_reviewed_apartment(self):
        # Assuming customer with id 1 and apartment with id 1 exist, and the customer has a reservation that ended before today's date
        self.assertEqual(customer_reviewed_apartment(1, 1, date.today(), 5, "Good apartment"), ReturnValue.OK)
        # Assuming customer with id 1 and apartment with id 999 do not exist
        self.assertEqual(customer_reviewed_apartment(1, 999, date.today(), 5, "Good apartment"), ReturnValue.NOT_EXISTS)
        # Assuming customer with id 999 and apartment with id 1 do not exist
        self.assertEqual(customer_reviewed_apartment(999, 1, date.today(), 5, "Good apartment"), ReturnValue.NOT_EXISTS)
        # Assuming customer with id 999 and apartment with id 999 do not exist
        self.assertEqual(customer_reviewed_apartment(999, 999, date.today(), 5, "Good apartment"), ReturnValue.NOT_EXISTS)
        # Assuming customer with id 1 and apartment with id 1 exist, but the customer does not have a reservation that ended before today's date
        self.assertEqual(customer_reviewed_apartment(1, 1, date.today(), 5, "Good apartment"), ReturnValue.NOT_EXISTS)
        # Assuming customer with id 1 and apartment with id 1 exist, but the rating is invalid
        self.assertEqual(customer_reviewed_apartment(1, 1, date.today(), 6, "Good apartment"), ReturnValue.BAD_PARAMS)
        # Assuming customer with id 1 and apartment with id 1 exist, but the review is invalid
        self.assertEqual(customer_reviewed_apartment(1, 1, date.today(), 5, ""), ReturnValue.BAD_PARAMS)
        
        
    def test_customer_updated_review(self):
        # Test updating an existing review
        result = customer_updated_review(1, 1, date(2022, 1, 1), 4, "Updated review")
        self.assertEqual(result, ReturnValue.OK)

        # Test updating a non-existing review
        result = customer_updated_review(2, 1, date(2022, 1, 1), 4, "Updated review")
        self.assertEqual(result, ReturnValue.NOT_EXISTS)

        # Test updating with invalid parameters
        result = customer_updated_review(1, 1, date(2022, 1, 1), 6, "Invalid rating")
        self.assertEqual(result, ReturnValue.BAD_PARAMS)

        # Test updating with foreign key violation
        result = customer_updated_review(1, 5, date(2022, 1, 1), 4, "Updated review")
        self.assertEqual(result, ReturnValue.NOT_EXISTS)

        # Test updating with an error
        result = customer_updated_review(1, 1, date(2022, 1, 1), 4, "Updated review")
        self.assertEqual(result, ReturnValue.ERROR)
    def test_get_apartment_rating(self):
        # Test getting the rating of an existing apartment
        self.assertEqual(get_apartment_rating(1), 4.5)

        # Test getting the rating of a non-existing apartment
        self.assertEqual(get_apartment_rating(4), 0.0)

if __name__ == "__main__":
    unittest.main()
