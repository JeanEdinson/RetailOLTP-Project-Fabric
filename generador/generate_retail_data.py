import os
import random
import argparse
from datetime import datetime, timedelta, date

import pyodbc
from faker import Faker
from dotenv import load_dotenv


# ============================================================
# CONFIGURACIÓN
# ============================================================

load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")

SQL_DRIVER = os.getenv(
    "SQL_DRIVER",
    "ODBC Driver 18 for SQL Server"
)

fake = Faker("es_ES")

Faker.seed(42)
random.seed(42)

DEFAULT_DIRTY_RATE = 0.10


# ============================================================
# CATÁLOGOS DE NEGOCIO
# ============================================================

CUSTOMER_SEGMENTS = [
    "Retail",
    "Wholesale",
    "Corporate",
    "Premium",
    "Online"
]


CATEGORIES = [
    "Electronics",
    "Computers",
    "Mobile",
    "Home",
    "Kitchen",
    "Sports",
    "Clothing",
    "Beauty",
    "Books",
    "Toys"
]


PRODUCT_CATALOG = {

    "Electronics": [
        "Wireless Headphones",
        "Bluetooth Speaker",
        "Soundbar",
        "Smart TV",
        "Digital Camera",
        "Wireless Earbuds",
        "Portable Speaker",
        "Home Theater System"
    ],

    "Computers": [
        "Laptop",
        "Desktop Computer",
        "Mechanical Keyboard",
        "Wireless Mouse",
        "Gaming Monitor",
        "USB Hub",
        "External SSD",
        "Laptop Stand",
        "Webcam",
        "Computer Speakers"
    ],

    "Mobile": [
        "Smartphone",
        "Tablet",
        "Smartwatch",
        "Phone Charger",
        "USB-C Cable",
        "Power Bank",
        "Phone Case",
        "Wireless Charger",
        "Screen Protector"
    ],

    "Home": [
        "Vacuum Cleaner",
        "Air Purifier",
        "Electric Fan",
        "Floor Lamp",
        "Table Lamp",
        "Digital Scale",
        "Storage Organizer",
        "Laundry Basket",
        "Humidifier"
    ],

    "Kitchen": [
        "Coffee Maker",
        "Air Fryer",
        "Electric Kettle",
        "Blender",
        "Toaster",
        "Microwave Oven",
        "Rice Cooker",
        "Food Processor",
        "Hand Mixer",
        "Kitchen Scale"
    ],

    "Sports": [
        "Running Shoes",
        "Yoga Mat",
        "Resistance Bands",
        "Sports Backpack",
        "Water Bottle",
        "Dumbbell Set",
        "Exercise Bike",
        "Training Gloves",
        "Football",
        "Basketball"
    ],

    "Clothing": [
        "Cotton T-Shirt",
        "Polo Shirt",
        "Hooded Sweatshirt",
        "Denim Jeans",
        "Casual Jacket",
        "Running Shorts",
        "Sports T-Shirt",
        "Winter Jacket",
        "Sneakers",
        "Baseball Cap"
    ],

    "Beauty": [
        "Facial Cleanser",
        "Moisturizing Cream",
        "Shampoo",
        "Conditioner",
        "Hair Dryer",
        "Electric Shaver",
        "Perfume",
        "Body Lotion",
        "Hair Straightener",
        "Makeup Brush Set"
    ],

    "Books": [
        "Business Management Book",
        "Programming Book",
        "Data Science Book",
        "History Book",
        "Personal Finance Book",
        "Marketing Book",
        "Psychology Book",
        "Leadership Book",
        "Science Book",
        "Language Learning Book"
    ],

    "Toys": [
        "Building Blocks",
        "Remote Control Car",
        "Board Game",
        "Puzzle Set",
        "Educational Toy",
        "Action Figure",
        "Toy Kitchen Set",
        "Stuffed Animal",
        "Kids Bicycle",
        "Art Set"
    ]
}


PRODUCT_VARIANTS = [
    "Standard",
    "Premium",
    "Pro",
    "Plus",
    "Advanced",
    "Classic"
]


PERU_CITIES = [
    ("Lima", "Lima"),
    ("Arequipa", "Arequipa"),
    ("Trujillo", "La Libertad"),
    ("Chiclayo", "Lambayeque"),
    ("Piura", "Piura"),
    ("Cusco", "Cusco"),
    ("Ica", "Ica"),
    ("Huancayo", "Junín"),
    ("Tacna", "Tacna"),
    ("Chimbote", "Áncash"),
    ("Cajamarca", "Cajamarca"),
    ("Pucallpa", "Ucayali"),
    ("Ayacucho", "Ayacucho"),
    ("Tarapoto", "San Martín"),
    ("Iquitos", "Loreto")
]


STORE_TYPES = [
    "Retail",
    "Mall",
    "Warehouse",
    "Outlet"
]


JOB_TITLES = [
    "Sales Associate",
    "Cashier",
    "Supervisor",
    "Store Manager",
    "Assistant Manager",
    "Sales Manager",
    "Inventory Clerk",
    "Customer Service Representative"
]


PAYMENT_METHODS = [
    "Card",
    "Cash",
    "Transfer"
]


PAYMENT_STATUSES = [
    "Paid",
    "Pending"
]


ORDER_STATUSES = [
    "Completed",
    "Pending",
    "Cancelled"
]


RETURN_STATUSES = [
    "Approved",
    "Pending",
    "Rejected"
]


RETURN_REASONS = [
    "Damaged",
    "Wrong product",
    "Customer request",
    "Defective",
    "Other"
]


# ============================================================
# CONEXIÓN
# ============================================================

def get_connection():

    if not all([
        SQL_SERVER,
        SQL_DATABASE
    ]):
        raise ValueError(
            "Faltan variables de conexión en el archivo .env"
        )

    connection_string = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )

    return pyodbc.connect(connection_string)


# ============================================================
# UTILIDADES
# ============================================================

def maybe_dirty(dirty_rate):
    return random.random() < dirty_rate


def random_date(start_date, end_date):

    delta = end_date - start_date

    if delta.days <= 0:
        return start_date

    return start_date + timedelta(
        days=random.randint(0, delta.days)
    )


def random_datetime(start_date, end_date):

    d = random_date(
        start_date,
        end_date
    )

    return datetime(
        d.year,
        d.month,
        d.day,
        random.randint(8, 21),
        random.randint(0, 59),
        random.randint(0, 59)
    )


def batch_insert(cursor, sql, rows, batch_size=1000):

    if not rows:
        return

    for i in range(
        0,
        len(rows),
        batch_size
    ):

        batch = rows[
            i:i + batch_size
        ]

        cursor.executemany(
            sql,
            batch
        )


def get_peru_city():

    return random.choice(
        PERU_CITIES
    )


# ============================================================
# DATOS SUCIOS / INCONSISTENCIAS
# ============================================================

def dirty_gender(value):

    male_values = [
        "M",
        "Male",
        "Masculino",
        "Hombre",
        "H"
    ]

    female_values = [
        "F",
        "Female",
        "Femenino",
        "Mujer"
    ]

    invalid_values = [
        "X",
        "Unknown",
        "N/A",
        "",
        " "
    ]

    if random.random() < 0.15:

        return random.choice(
            invalid_values
        )

    if value == "M":

        return random.choice(
            male_values
        )

    return random.choice(
        female_values
    )


def dirty_status(value):

    mappings = {

        "Active": [
            "Active",
            "ACTIVE",
            "active",
            "Activo",
            "ACTIVO",
            "A"
        ],

        "Inactive": [
            "Inactive",
            "INACTIVE",
            "inactive",
            "Inactivo",
            "INACTIVO",
            "I"
        ],

        "Completed": [
            "Completed",
            "COMPLETED",
            "completed",
            "Completado",
            "COMPLETADO"
        ],

        "Pending": [
            "Pending",
            "PENDING",
            "pending",
            "Pendiente",
            "PENDIENTE"
        ],

        "Cancelled": [
            "Cancelled",
            "CANCELLED",
            "cancelled",
            "Cancelado",
            "CANCELADO"
        ],

        "Approved": [
            "Approved",
            "APPROVED",
            "approved",
            "Aprobado",
            "APROBADO"
        ],

        "Rejected": [
            "Rejected",
            "REJECTED",
            "rejected",
            "Rechazado",
            "RECHAZADO"
        ]
    }

    if random.random() < 0.10:

        return random.choice([
            None,
            "",
            "UNKNOWN",
            "Unknown",
            "N/A"
        ])

    values = mappings.get(value)

    if values:

        return random.choice(
            values
        )

    return value


def dirty_payment_method():

    return random.choice([
        "Card",
        "CARD",
        "Tarjeta",
        "Credit Card",
        "Debit Card",
        "Cash",
        "CASH",
        "Efectivo",
        "Transfer",
        "TRANSFER",
        "Transferencia"
    ])


def dirty_payment_status():

    return random.choice([
        "Paid",
        "PAID",
        "paid",
        "Pagado",
        "PAGADO",
        "P",
        "Pending",
        "PENDING",
        "Pendiente"
    ])


def dirty_address_type():

    return random.choice([
        "Home",
        "HOME",
        "home",
        "Casa",
        "Domicilio",
        "Work",
        "WORK",
        "Trabajo"
    ])


def dirty_country():

    return random.choice([
        "Peru",
        "PERU",
        "peru",
        "Perú",
        "PE",
        "Per"
    ])


def dirty_text(value):

    if value is None:
        return None

    return random.choice([
        value.upper(),
        value.lower(),
        f" {value}",
        f"{value} ",
        f"  {value}  ",
        value.replace(" ", ""),
        value
    ])


def dirty_email(email):

    return random.choice([
        email.upper(),
        f" {email}",
        f"{email} ",
        email.replace("@", ""),
        email.replace(".", ""),
        None,
        ""
    ])


def dirty_phone(phone):

    return random.choice([
        phone,
        f" {phone}",
        f"{phone} ",
        phone.replace(" ", ""),
        phone.replace("+", ""),
        f"({phone[:3]}){phone[3:]}",
        None
    ])


def dirty_sku(sku):

    return random.choice([
        sku.lower(),
        sku.upper(),
        f" {sku}",
        f"{sku} ",
        sku.replace("-", ""),
        sku.replace("-", " "),
        sku
    ])


# ============================================================
# GENERACIÓN DE MASTER DATA
# ============================================================

def generate_customer_segments(cursor):

    data = [
        ("Retail",),
        ("Wholesale",),
        ("Corporate",),
        ("Premium",),
        ("Online",)
    ]

    sql = """
        INSERT INTO CustomerSegments
        (
            SegmentName
        )
        VALUES (?)
    """

    batch_insert(
        cursor,
        sql,
        data
    )

    print(
        f"CustomerSegments: {len(data)}"
    )


def generate_categories(cursor):

    rows = []

    for category in CATEGORIES:

        name = category

        if maybe_dirty(
            DEFAULT_DIRTY_RATE
        ):

            dirty_type = random.randint(
                1,
                4
            )

            if dirty_type == 1:
                name = name.upper()

            elif dirty_type == 2:
                name = f" {name}"

            elif dirty_type == 3:
                name = name.lower()

            elif dirty_type == 4:

                if name == "Electronics":
                    name = "Electrónica"

        rows.append(
            (name,)
        )

    sql = """
        INSERT INTO Categories
        (
            CategoryName
        )
        VALUES (?)
    """

    batch_insert(
        cursor,
        sql,
        rows
    )

    print(
        f"Categories: {len(rows)}"
    )


def generate_suppliers(
    cursor,
    count=30
):

    rows = []

    for _ in range(count):

        name = fake.company()

        email = fake.company_email()

        phone = fake.phone_number()

        city, _ = get_peru_city()

        country = "Peru"

        if maybe_dirty(
            DEFAULT_DIRTY_RATE
        ):

            dirty_type = random.randint(
                1,
                4
            )

            if dirty_type == 1:
                name = dirty_text(name)

            elif dirty_type == 2:
                email = dirty_email(email)

            elif dirty_type == 3:
                phone = dirty_phone(phone)

            elif dirty_type == 4:
                country = dirty_country()

        rows.append((
            name,
            fake.name(),
            email,
            phone,
            city,
            country
        ))

    sql = """
        INSERT INTO Suppliers
        (
            SupplierName,
            ContactName,
            Email,
            Phone,
            City,
            Country
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """

    batch_insert(
        cursor,
        sql,
        rows
    )

    print(
        f"Suppliers: {len(rows)}"
    )


def generate_customers(
    cursor,
    count=1000
):

    segment_ids = [
        row[0]
        for row in cursor.execute(
            """
            SELECT CustomerSegmentID
            FROM CustomerSegments
            """
        ).fetchall()
    ]

    rows = []

    for _ in range(count):

        first_name = fake.first_name()

        last_name = fake.last_name()

        email = fake.email()

        phone = fake.phone_number()

        birth_date = random_date(
            date(1950, 1, 1),
            date(2004, 12, 31)
        )

        gender = random.choice([
            "M",
            "F"
        ])

        registration_date = random_date(
            date(2021, 1, 1),
            date(2026, 8, 31)
        )

        status = random.choice([
            "Active",
            "Inactive"
        ])

        if maybe_dirty(
            DEFAULT_DIRTY_RATE
        ):

            dirty_type = random.randint(
                1,
                6
            )

            if dirty_type == 1:

                gender = dirty_gender(
                    gender
                )

            elif dirty_type == 2:

                email = dirty_email(
                    email
                )

            elif dirty_type == 3:

                phone = dirty_phone(
                    phone
                )

            elif dirty_type == 4:

                status = dirty_status(
                    status
                )

            elif dirty_type == 5:

                registration_date = None

            elif dirty_type == 6:

                birth_date = None

        rows.append((
            random.choice(segment_ids),
            first_name,
            last_name,
            email,
            phone,
            birth_date,
            gender,
            registration_date,
            status
        ))

    sql = """
        INSERT INTO Customers
        (
            CustomerSegmentID,
            FirstName,
            LastName,
            Email,
            Phone,
            BirthDate,
            Gender,
            RegistrationDate,
            Status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    batch_insert(
        cursor,
        sql,
        rows
    )

    print(
        f"Customers: {len(rows)}"
    )


def generate_addresses(
    cursor,
    addresses_per_customer=1
):

    customers = [
        row[0]
        for row in cursor.execute(
            """
            SELECT CustomerID
            FROM Customers
            """
        ).fetchall()
    ]

    rows = []

    for customer_id in customers:

        for _ in range(
            addresses_per_customer
        ):

            address_type = random.choice([
                "Home",
                "Work"
            ])

            city, state = get_peru_city()

            postal_code = fake.postcode()

            country = "Peru"

            is_primary = True

            if maybe_dirty(
                DEFAULT_DIRTY_RATE
            ):

                dirty_type = random.randint(
                    1,
                    4
                )

                if dirty_type == 1:

                    address_type = (
                        dirty_address_type()
                    )

                elif dirty_type == 2:

                    city = dirty_text(
                        city
                    )

                elif dirty_type == 3:

                    country = dirty_country()

                elif dirty_type == 4:

                    postal_code = None

            rows.append((
                customer_id,
                address_type,
                fake.street_address(),
                None,
                city,
                state,
                postal_code,
                country,
                is_primary
            ))

    sql = """
        INSERT INTO CustomerAddresses
        (
            CustomerID,
            AddressType,
            AddressLine1,
            AddressLine2,
            City,
            State,
            PostalCode,
            Country,
            IsPrimary
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    batch_insert(
        cursor,
        sql,
        rows
    )

    print(
        f"CustomerAddresses: {len(rows)}"
    )


def generate_stores(
    cursor,
    count=15
):

    rows = []

    for i in range(count):

        store_code = (
            f"ST-{i + 1:03d}"
        )

        city, state = get_peru_city()

        store_name = (
            f"{city} Store "
            f"{i + 1:03d}"
        )

        opening_date = random_date(
            date(2018, 1, 1),
            date(2025, 12, 31)
        )

        store_type = random.choice(
            STORE_TYPES
        )

        status = random.choice([
            "Active",
            "Inactive"
        ])

        if maybe_dirty(
            DEFAULT_DIRTY_RATE
        ):

            if random.random() < 0.5:

                store_type = dirty_text(
                    store_type
                )

            else:

                status = dirty_status(
                    status
                )

        rows.append((
            store_code,
            store_name,
            fake.street_address(),
            city,
            state,
            "Peru",
            opening_date,
            store_type,
            status
        ))

    sql = """
        INSERT INTO Stores
        (
            StoreCode,
            StoreName,
            Address,
            City,
            State,
            Country,
            OpeningDate,
            StoreType,
            Status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    batch_insert(
        cursor,
        sql,
        rows
    )

    print(
        f"Stores: {len(rows)}"
    )


def generate_employees(
    cursor,
    count=100
):

    stores = [
        row[0]
        for row in cursor.execute(
            """
            SELECT StoreID
            FROM Stores
            """
        ).fetchall()
    ]

    rows = []

    for _ in range(count):

        hire_date = random_date(
            date(2018, 1, 1),
            date(2025, 12, 31)
        )

        status = random.choice([
            "Active",
            "Inactive"
        ])

        if maybe_dirty(
            DEFAULT_DIRTY_RATE
        ):

            status = dirty_status(
                status
            )

        rows.append((
            random.choice(stores),
            fake.first_name(),
            fake.last_name(),
            fake.company_email(),
            random.choice(JOB_TITLES),
            hire_date,
            status
        ))

    sql = """
        INSERT INTO Employees
        (
            StoreID,
            FirstName,
            LastName,
            Email,
            JobTitle,
            HireDate,
            Status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    batch_insert(
        cursor,
        sql,
        rows
    )

    print(
        f"Employees: {len(rows)}"
    )


# ============================================================
# PRODUCTOS
# ============================================================

def generate_product_name(
    category_name,
    product_number
):

    product_type = random.choice(
        PRODUCT_CATALOG[
            category_name
        ]
    )

    variant = random.choice(
        PRODUCT_VARIANTS
    )

    return (
        f"{product_type} "
        f"{variant} "
        f"{product_number:04d}"
    )


def generate_products(
    cursor,
    count=500
):

    categories = cursor.execute(
        """
        SELECT
            CategoryID,
            CategoryName
        FROM Categories
        """
    ).fetchall()

    suppliers = [
        row[0]
        for row in cursor.execute(
            """
            SELECT SupplierID
            FROM Suppliers
            """
        ).fetchall()
    ]

    rows = []

    for i in range(count):

        category_id, category_name = (
            random.choice(categories)
        )

        clean_category = (
            str(category_name)
            .strip()
            .lower()
        )

        category_mapping = {
            "electronics": "Electronics",
            "electrónica": "Electronics",
            "computers": "Computers",
            "computers": "Computers",
            "mobile": "Mobile",
            "home": "Home",
            "kitchen": "Kitchen",
            "sports": "Sports",
            "clothing": "Clothing",
            "beauty": "Beauty",
            "books": "Books",
            "toys": "Toys"
        }

        normalized_category = (
            category_mapping.get(
                clean_category,
                "Electronics"
            )
        )

        product_name = (
            generate_product_name(
                normalized_category,
                i + 1
            )
        )

        sku = (
            f"SKU-"
            f"{normalized_category[:3].upper()}-"
            f"{i + 1:06d}"
        )

        unit_price = round(
            random.uniform(
                10,
                2000
            ),
            2
        )

        cost_price = round(
            unit_price *
            random.uniform(
                0.45,
                0.80
            ),
            2
        )

        units_per_package = random.choice([
            1,
            2,
            4,
            6,
            12
        ])

        status = random.choice([
            "Active",
            "Inactive"
        ])

        if maybe_dirty(
            DEFAULT_DIRTY_RATE
        ):

            dirty_type = random.randint(
                1,
                4
            )

            if dirty_type == 1:

                sku = dirty_sku(
                    sku
                )

            elif dirty_type == 2:

                unit_price = None

            elif dirty_type == 3:

                cost_price = round(
                    unit_price *
                    random.uniform(
                        1.05,
                        1.20
                    ),
                    2
                )

            elif dirty_type == 4:

                status = dirty_status(
                    status
                )

        rows.append((
            category_id,
            random.choice(suppliers),
            sku,
            product_name,
            unit_price,
            cost_price,
            units_per_package,
            status
        ))

    sql = """
        INSERT INTO Products
        (
            CategoryID,
            SupplierID,
            SKU,
            ProductName,
            UnitPrice,
            CostPrice,
            UnitsPerPackage,
            Status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    batch_insert(
        cursor,
        sql,
        rows
    )

    print(
        f"Products: {len(rows)}"
    )


# ============================================================
# TRANSACCIONES
# ============================================================

def generate_orders(
    cursor,
    order_count
):

    customers = cursor.execute(
        """
        SELECT
            CustomerID,
            RegistrationDate
        FROM Customers
        """
    ).fetchall()

    stores = [
        row[0]
        for row in cursor.execute(
            """
            SELECT StoreID
            FROM Stores
            """
        ).fetchall()
    ]

    employees = cursor.execute(
        """
        SELECT
            EmployeeID,
            StoreID
        FROM Employees
        """
    ).fetchall()

    products = cursor.execute(
        """
        SELECT
            ProductID,
            UnitPrice
        FROM Products
        """
    ).fetchall()

    if not customers:
        raise ValueError(
            "No existen Customers."
        )

    if not stores:
        raise ValueError(
            "No existen Stores."
        )

    if not employees:
        raise ValueError(
            "No existen Employees."
        )

    if not products:
        raise ValueError(
            "No existen Products."
        )

    order_sql = """
        INSERT INTO Orders
        (
            CustomerID,
            StoreID,
            EmployeeID,
            OrderDate,
            OrderStatus,
            Subtotal,
            DiscountAmount,
            TaxAmount,
            ShippingAmount,
            TotalAmount
        )
        OUTPUT INSERTED.OrderID
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    detail_sql = """
        INSERT INTO OrderDetails
        (
            OrderID,
            ProductID,
            Quantity,
            UnitPrice,
            DiscountPercent,
            DiscountAmount,
            TaxAmount,
            LineTotal
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    inserted_order_ids = []

    total_detail_count = 0

    for _ in range(order_count):

        customer_id, registration_date = (
            random.choice(customers)
        )

        store_id = random.choice(
            stores
        )

        compatible_employees = [
            employee
            for employee in employees
            if employee[1] == store_id
        ]

        if compatible_employees:

            employee_id = random.choice(
                compatible_employees
            )[0]

        else:

            employee_id = random.choice(
                employees
            )[0]

        if registration_date:

            start_date = max(
                registration_date,
                date(2023, 1, 1)
            )

        else:

            start_date = date(
                2023,
                1,
                1
            )

        order_date = random_datetime(
            start_date,
            date(2026, 8, 31)
        )

        order_status = random.choice(
            ORDER_STATUSES
        )

        selected_products = random.sample(
            products,
            random.randint(1, 5)
        )

        details = []

        subtotal = 0
        discount_total = 0
        tax_total = 0

        for product_id, product_price in selected_products:

            if product_price is None:

                product_price = round(
                    random.uniform(
                        10,
                        500
                    ),
                    2
                )

            quantity = random.randint(
                1,
                5
            )

            unit_price = float(
                product_price
            )

            discount_percent = random.choice([
                0,
                0,
                5,
                10,
                15
            ])

            gross = (
                quantity *
                unit_price
            )

            discount_amount = round(
                gross *
                discount_percent /
                100,
                2
            )

            net = (
                gross -
                discount_amount
            )

            tax_amount = round(
                net * 0.18,
                2
            )

            line_total = round(
                net +
                tax_amount,
                2
            )

            details.append({
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_percent": discount_percent,
                "discount_amount": discount_amount,
                "tax_amount": tax_amount,
                "line_total": line_total
            })

            subtotal += gross

            discount_total += (
                discount_amount
            )

            tax_total += tax_amount

        shipping = random.choice([
            0,
            0,
            5,
            10,
            15
        ])

        total = (
            subtotal
            - discount_total
            + tax_total
            + shipping
        )

        # ====================================================
        # INCONSISTENCIAS EN ORDER
        # ====================================================

        dirty_order = maybe_dirty(
            DEFAULT_DIRTY_RATE
        )

        if dirty_order:

            dirty_type = random.randint(
                1,
                7
            )

            if dirty_type == 1:

                order_status = dirty_status(
                    order_status
                )

            elif dirty_type == 2:

                if registration_date:

                    previous_date = max(
                        date(2020, 1, 1),
                        registration_date -
                        timedelta(
                            days=random.randint(
                                1,
                                500
                            )
                        )
                    )

                    order_date = datetime(
                        previous_date.year,
                        previous_date.month,
                        previous_date.day,
                        10,
                        30
                    )

            elif dirty_type == 3:

                total = round(
                    total *
                    random.uniform(
                        0.80,
                        1.20
                    ),
                    2
                )

            elif dirty_type == 4:

                discount_total = round(
                    discount_total * 1.5,
                    2
                )

            elif dirty_type == 5:

                shipping = None

            elif dirty_type == 6:

                subtotal = round(
                    subtotal * 1.10,
                    2
                )

            elif dirty_type == 7:

                employee_id = random.choice(
                    employees
                )[0]

        order_row = (
            customer_id,
            store_id,
            employee_id,
            order_date,
            order_status,
            round(subtotal, 2),
            round(discount_total, 2),
            round(tax_total, 2),
            shipping,
            round(total, 2)
        )

        cursor.execute(
            order_sql,
            order_row
        )

        order_id = cursor.fetchone()[0]

        inserted_order_ids.append(
            order_id
        )

        detail_rows = []

        for detail in details:

            quantity = detail[
                "quantity"
            ]

            unit_price = detail[
                "unit_price"
            ]

            discount_percent = detail[
                "discount_percent"
            ]

            discount_amount = detail[
                "discount_amount"
            ]

            tax_amount = detail[
                "tax_amount"
            ]

            line_total = detail[
                "line_total"
            ]

            if (
                dirty_order
                and
                random.random() < 0.25
            ):

                dirty_detail_type = (
                    random.randint(1, 5)
                )

                if dirty_detail_type == 1:

                    quantity = -random.randint(
                        1,
                        3
                    )

                elif dirty_detail_type == 2:

                    discount_percent = (
                        random.choice([
                            110,
                            150,
                            -10
                        ])
                    )

                elif dirty_detail_type == 3:

                    line_total = round(
                        line_total * 0.7,
                        2
                    )

                elif dirty_detail_type == 4:

                    unit_price = round(
                        unit_price * 1.3,
                        2
                    )

                elif dirty_detail_type == 5:

                    discount_amount = None

            detail_rows.append((
                order_id,
                detail[
                    "product_id"
                ],
                quantity,
                unit_price,
                discount_percent,
                discount_amount,
                tax_amount,
                line_total
            ))

        batch_insert(
            cursor,
            detail_sql,
            detail_rows
        )

        total_detail_count += len(
            detail_rows
        )

    print(
        f"Orders: {len(inserted_order_ids)}"
    )

    print(
        f"OrderDetails: {total_detail_count}"
    )

    return inserted_order_ids


# ============================================================
# PAYMENTS
# ============================================================

def generate_payments(
    cursor,
    order_ids
):

    rows = []

    for order_id in order_ids:

        order = cursor.execute(
            """
            SELECT
                OrderDate,
                TotalAmount
            FROM Orders
            WHERE OrderID = ?
            """,
            order_id
        ).fetchone()

        if not order:
            continue

        order_date, total_amount = order

        payment_date = (
            order_date +
            timedelta(
                minutes=random.randint(
                    1,
                    1440
                )
            )
        )

        payment_status = random.choice(
            PAYMENT_STATUSES
        )

        payment_method = random.choice(
            PAYMENT_METHODS
        )

        amount = total_amount

        if maybe_dirty(
            DEFAULT_DIRTY_RATE
        ):

            dirty_type = random.randint(
                1,
                4
            )

            if dirty_type == 1:

                payment_method = (
                    dirty_payment_method()
                )

            elif dirty_type == 2:

                payment_status = (
                    dirty_payment_status()
                )

            elif dirty_type == 3:

                payment_date = (
                    order_date -
                    timedelta(
                        days=random.randint(
                            1,
                            10
                        )
                    )
                )

            elif dirty_type == 4:

                amount = round(
                    float(
                        total_amount or 0
                    ) *
                    random.uniform(
                        0.7,
                        1.3
                    ),
                    2
                )

        rows.append((
            order_id,
            payment_date,
            payment_method,
            payment_status,
            amount,
            fake.uuid4()
        ))

    sql = """
        INSERT INTO Payments
        (
            OrderID,
            PaymentDate,
            PaymentMethod,
            PaymentStatus,
            Amount,
            TransactionReference
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """

    batch_insert(
        cursor,
        sql,
        rows
    )

    print(
        f"Payments: {len(rows)}"
    )


# ============================================================
# RETURNS
# ============================================================

def generate_returns(
    cursor,
    return_rate=0.08
):

    details = cursor.execute(
        """
        SELECT
            OrderDetailID,
            OrderID,
            Quantity
        FROM OrderDetails
        WHERE Quantity > 0
        """
    ).fetchall()

    rows = []

    for detail_id, order_id, quantity in details:

        if random.random() > return_rate:
            continue

        order = cursor.execute(
            """
            SELECT OrderDate
            FROM Orders
            WHERE OrderID = ?
            """,
            order_id
        ).fetchone()

        if not order:
            continue

        order_date = order[0]

        return_date = (
            order_date +
            timedelta(
                days=random.randint(
                    1,
                    60
                )
            )
        )

        return_quantity = random.randint(
            1,
            max(1, quantity)
        )

        refund_amount = round(
            random.uniform(
                10,
                500
            ),
            2
        )

        return_status = random.choice(
            RETURN_STATUSES
        )

        if maybe_dirty(
            DEFAULT_DIRTY_RATE
        ):

            dirty_type = random.randint(
                1,
                4
            )

            if dirty_type == 1:

                return_date = (
                    order_date -
                    timedelta(
                        days=random.randint(
                            1,
                            10
                        )
                    )
                )

            elif dirty_type == 2:

                return_quantity = (
                    quantity +
                    random.randint(
                        1,
                        5
                    )
                )

            elif dirty_type == 3:

                refund_amount = round(
                    refund_amount * 1.5,
                    2
                )

            elif dirty_type == 4:

                return_status = dirty_status(
                    return_status
                )

        rows.append((
            detail_id,
            return_date,
            return_quantity,
            random.choice(
                RETURN_REASONS
            ),
            refund_amount,
            return_status
        ))

    sql = """
        INSERT INTO Returns
        (
            OrderDetailID,
            ReturnDate,
            Quantity,
            ReturnReason,
            RefundAmount,
            ReturnStatus
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """

    batch_insert(
        cursor,
        sql,
        rows
    )

    print(
        f"Returns: {len(rows)}"
    )


# ============================================================
# ACTUALIZACIONES
# ============================================================

def update_customers(
    cursor,
    count
):

    customers = cursor.execute(
        """
        SELECT TOP (?)
            CustomerID
        FROM Customers
        ORDER BY NEWID()
        """,
        count
    ).fetchall()

    for row in customers:

        customer_id = row[0]

        update_type = random.randint(
            1,
            4
        )

        if update_type == 1:

            cursor.execute(
                """
                UPDATE Customers
                SET Email = ?
                WHERE CustomerID = ?
                """,
                fake.email(),
                customer_id
            )

        elif update_type == 2:

            cursor.execute(
                """
                UPDATE Customers
                SET Phone = ?
                WHERE CustomerID = ?
                """,
                fake.phone_number(),
                customer_id
            )

        elif update_type == 3:

            cursor.execute(
                """
                UPDATE Customers
                SET Status = ?
                WHERE CustomerID = ?
                """,
                random.choice([
                    "Active",
                    "Inactive"
                ]),
                customer_id
            )

        elif update_type == 4:

            cursor.execute(
                """
                UPDATE Customers
                SET CustomerSegmentID = (
                    SELECT TOP 1
                        CustomerSegmentID
                    FROM CustomerSegments
                    ORDER BY NEWID()
                )
                WHERE CustomerID = ?
                """,
                customer_id
            )

    print(
        f"Customers updated: {len(customers)}"
    )


def update_addresses(
    cursor,
    count
):

    addresses = cursor.execute(
        """
        SELECT TOP (?)
            CustomerAddressID
        FROM CustomerAddresses
        ORDER BY NEWID()
        """,
        count
    ).fetchall()

    for row in addresses:

        address_id = row[0]

        city, _ = get_peru_city()

        cursor.execute(
            """
            UPDATE CustomerAddresses
            SET
                AddressLine1 = ?,
                City = ?,
                PostalCode = ?
            WHERE CustomerAddressID = ?
            """,
            fake.street_address(),
            city,
            fake.postcode(),
            address_id
        )

    print(
        f"CustomerAddresses updated: "
        f"{len(addresses)}"
    )


def update_products(
    cursor,
    count
):

    products = cursor.execute(
        """
        SELECT TOP (?)
            ProductID
        FROM Products
        ORDER BY NEWID()
        """,
        count
    ).fetchall()

    for row in products:

        product_id = row[0]

        update_type = random.randint(
            1,
            3
        )

        if update_type == 1:

            cursor.execute(
                """
                UPDATE Products
                SET UnitPrice =
                    ROUND(
                        UnitPrice * ?,
                        2
                    )
                WHERE ProductID = ?
                """,
                random.uniform(
                    0.90,
                    1.15
                ),
                product_id
            )

        elif update_type == 2:

            cursor.execute(
                """
                UPDATE Products
                SET CategoryID = (
                    SELECT TOP 1
                        CategoryID
                    FROM Categories
                    ORDER BY NEWID()
                )
                WHERE ProductID = ?
                """,
                product_id
            )

        else:

            cursor.execute(
                """
                UPDATE Products
                SET Status = ?
                WHERE ProductID = ?
                """,
                random.choice([
                    "Active",
                    "Inactive"
                ]),
                product_id
            )

    print(
        f"Products updated: {len(products)}"
    )


def update_orders(
    cursor,
    count
):

    orders = cursor.execute(
        """
        SELECT TOP (?)
            OrderID
        FROM Orders
        ORDER BY NEWID()
        """,
        count
    ).fetchall()

    for row in orders:

        order_id = row[0]

        cursor.execute(
            """
            UPDATE Orders
            SET OrderStatus = ?
            WHERE OrderID = ?
            """,
            random.choice(
                ORDER_STATUSES
            ),
            order_id
        )

    print(
        f"Orders updated: {len(orders)}"
    )


def update_order_details(
    cursor,
    count
):

    details = cursor.execute(
        """
        SELECT TOP (?)
            OrderDetailID
        FROM OrderDetails
        ORDER BY NEWID()
        """,
        count
    ).fetchall()

    for row in details:

        detail_id = row[0]

        cursor.execute(
            """
            UPDATE OrderDetails
            SET Quantity = ?
            WHERE OrderDetailID = ?
            """,
            random.randint(
                1,
                10
            ),
            detail_id
        )

    print(
        f"OrderDetails updated: "
        f"{len(details)}"
    )


def perform_updates(
    cursor,
    update_count
):

    print(
        "\n=== ACTUALIZANDO DATOS EXISTENTES ==="
    )

    update_customers(
        cursor,
        max(
            1,
            update_count
        )
    )

    update_addresses(
        cursor,
        max(
            1,
            update_count // 2
        )
    )

    update_products(
        cursor,
        max(
            1,
            update_count
        )
    )

    update_orders(
        cursor,
        max(
            1,
            update_count
        )
    )

    update_order_details(
        cursor,
        max(
            1,
            update_count
        )
    )


# ============================================================
# GENERACIÓN COMPLETA
# ============================================================

def generate_data(
    orders_count,
    customer_count,
    product_count,
    supplier_count,
    store_count,
    employee_count
):

    connection = get_connection()

    try:

        cursor = connection.cursor()

        print(
            "\n========================================"
        )

        print(
            "GENERANDO DATOS RETAIL"
        )

        print(
            "========================================\n"
        )

        print(
            "Generando master data...\n"
        )

        generate_customer_segments(
            cursor
        )

        generate_categories(
            cursor
        )

        generate_suppliers(
            cursor,
            supplier_count
        )

        generate_customers(
            cursor,
            customer_count
        )

        generate_addresses(
            cursor,
            addresses_per_customer=1
        )

        generate_stores(
            cursor,
            store_count
        )

        generate_employees(
            cursor,
            employee_count
        )

        generate_products(
            cursor,
            product_count
        )

        connection.commit()

        print(
            "\nMaster data generado correctamente."
        )

        print(
            "\nGenerando transacciones...\n"
        )

        order_ids = generate_orders(
            cursor,
            orders_count
        )

        generate_payments(
            cursor,
            order_ids
        )

        generate_returns(
            cursor
        )

        connection.commit()

        print(
            "\n========================================"
        )

        print(
            "GENERACIÓN COMPLETADA"
        )

        print(
            "========================================"
        )

    except Exception as e:

        connection.rollback()

        print(
            "\nERROR:"
        )

        print(e)

        raise

    finally:

        connection.close()


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Generador de datos RetailOLTP"
    )

    parser.add_argument(
        "--mode",
        choices=[
            "generate",
            "update",
            "generate-and-update"
        ],
        default="generate"
    )

    parser.add_argument(
        "--orders",
        type=int,
        default=10000
    )

    parser.add_argument(
        "--customers",
        type=int,
        default=1000
    )

    parser.add_argument(
        "--products",
        type=int,
        default=500
    )

    parser.add_argument(
        "--suppliers",
        type=int,
        default=30
    )

    parser.add_argument(
        "--stores",
        type=int,
        default=15
    )

    parser.add_argument(
        "--employees",
        type=int,
        default=100
    )

    parser.add_argument(
        "--updates",
        type=int,
        default=100
    )

    args = parser.parse_args()

    if args.mode in [
        "generate",
        "generate-and-update"
    ]:

        generate_data(
            orders_count=args.orders,
            customer_count=args.customers,
            product_count=args.products,
            supplier_count=args.suppliers,
            store_count=args.stores,
            employee_count=args.employees
        )

    if args.mode in [
        "update",
        "generate-and-update"
    ]:

        connection = get_connection()

        try:

            cursor = connection.cursor()

            perform_updates(
                cursor,
                args.updates
            )

            connection.commit()

            print(
                "\nActualizaciones completadas."
            )

        except Exception as e:

            connection.rollback()

            print(
                "\nERROR:"
            )

            print(e)

            raise

        finally:

            connection.close()


if __name__ == "__main__":
    main()
