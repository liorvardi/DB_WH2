import unittest
from datetime import date
from Solution import *
from Business.Owner import Owner
from Business.Apartment import Apartment

class TestSolution(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        create_tables()

    @classmethod
    def tearDownClass(cls):
        # This method will be called once after all tests are executed
        # Clean up your test environment
        drop_tables()
        
    def test_get_all_location_owners(self):
        # Test getting owners for a specific location
        # Add some test data first and then check if the function returns expected results
        # For example, let's assume we have owners with apartments in different cities
        # We'll need to add some test data for this scenario and then call the function

        # Add some sample owners and their apartments in different cities
        add_owner(Owner(1, "John Doe"))
        add_owner(Owner(2, "Jane Smith"))
        add_apartment(Apartment(1, "123 Main St", "New York", "USA", 100))
        add_apartment(Apartment(2, "456 Elm St", "Los Angeles", "USA", 120))
        add_apartment(Apartment(3, "789 Oak St", "Chicago", "USA", 150))
        add_owner(Owner(3, "Mike Johnson"))
        add_apartment(Apartment(4, "987 Pine St", "New York", "USA", 110))
        add_apartment(Apartment(5, "654 Maple St", "Los Angeles", "USA", 130))

        # Now call the function to get owners for a specific location
        owners = get_all_location_owners()

        # Assert that we get the expected number of owners for the specific location
        # You should adjust these assertions based on the expected results
        self.assertEqual(len(owners), 2)
        # Add more specific assertions if needed based on your application logic

    def test_best_value_for_money(self):
        # Test finding the best value apartment
        # Add some test data first and then check if the function returns expected results
        # For example, let's assume we have apartments with different ratings and prices
        # We'll need to add some test data for this scenario and then call the function

        # Add some sample apartments with reviews and reservations
        add_apartment(Apartment(1, "123 Main St", "New York", "USA", 100))
        add_apartment(Apartment(2, "456 Elm St", "Los Angeles", "USA", 120))
        add_apartment(Apartment(3, "789 Oak St", "Chicago", "USA", 150))

        # Add some sample reservations for the apartments
        customer_made_reservation(1, 1, date(2023, 1, 1), date(2023, 2, 1), 200)
        customer_made_reservation(1, 2,date(2023, 1, 2), date(2023, 2, 1), 180)
        customer_made_reservation(1, 3, date(2023, 1, 3), date(2023, 2, 1), 250)

        # Add some sample reviews for the apartments
        customer_reviewed_apartment(1, 1, date.today(), 8, "Great apartment")
        customer_reviewed_apartment(1, 2, date.today(), 7, "Nice place")
        customer_reviewed_apartment(1, 3, date.today(), 9, "Fantastic view")

        # Now call the function to get the best value apartment
        best_apartment = best_value_for_money()

        # Assert that we get the expected best value apartment
        # You should adjust these assertions based on the expected results
        self.assertEqual(best_apartment.get_id(), 2)
        # Add more specific assertions if needed based on your application logic

    def test_profit_per_month(self):
        # Test getting profit per month for a specific year
        # Add some test data first and then check if the function returns expected results
        # For example, let's assume we have reservations with different prices and dates
        # We'll need to add some test data for this scenario and then call the function

        # Add some sample reservations with prices and dates
        customer_made_reservation(1, 1, date(2024, 1, 1), date(2024, 1, 5), 500)
        customer_made_reservation(1, 2, date(2024, 2, 1), date(2024, 2, 5), 600)
        customer_made_reservation(1, 3, date(2024, 3, 1), date(2024, 3, 5), 700)

        # Now call the function to get profit per month for the specific year
        profits = profit_per_month(2024)

        # Assert that we get the expected profit per month
        # You should adjust these assertions based on the expected results
        self.assertEqual(len(profits), 3)
        # Add more specific assertions if needed based on your application logic

    def test_get_apartment_recommendation(self):
        # Test getting apartment recommendations for a customer
        # Add some test data first and then check if the function returns expected results
        # For example, let's assume we have customers who reviewed different apartments
        # We'll need to add some test data for this scenario and then call the function

        # Add some sample apartments with reviews
        add_apartment(Apartment(1, "123 Main St", "New York", "USA", 100))
        add_apartment(Apartment(2, "456 Elm St", "Los Angeles", "USA", 120))
        add_apartment(Apartment(3, "789 Oak St", "Chicago", "USA", 150))

        # Add some sample reviews for the apartments by different customers
        customer_reviewed_apartment(1, 1, date.today(), 8, "Great apartment")
        customer_reviewed_apartment(1, 2, date.today(), 7, "Nice place")
        customer_reviewed_apartment(1, 3, date.today(), 9, "Fantastic view")
        customer_reviewed_apartment(2, 1, date.today(), 7, "Good location")
        customer_reviewed_apartment(2, 2, date.today(), 6, "Could be better")
        customer_reviewed_apartment(3, 2, date.today(), 9, "Loved it")

        # Now call the function to get apartment recommendations for a customer
        recommendations = get_apartment_recommendation(1)

        # Assert that we get the expected apartment recommendations
        # You should adjust these assertions based on the expected results
        self.assertEqual(len(recommendations), 2)
        # Add more specific assertions if needed based on your application logic

if __name__ == '__main__':
    unittest.main()