
bronze_customers_schema = [
    {'name': 'ClientId', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
]

bronze_sales_schema = [
    {'name': 'CustomerId', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Product', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Price', 'type': 'STRING', 'mode': 'NULLABLE'},
]

user_profiles_schema = [
    {'name': 'email', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'full_name', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'state', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'birth_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'phone_number', 'type': 'STRING', 'mode': 'REQUIRED'},
]