"""
Database seeder module.
Populates the e-commerce database with realistic sample data for testing and development.
"""

import sqlite3
import random
import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional, Tuple
import hashlib
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseSeeder:
    """
    Handles population of the e-commerce database with sample data.
    
    This class generates realistic test data that maintains referential
    integrity and provides a good dataset for testing queries.
    """
   
    def __init__(self, connection: sqlite3.Connection):
        """
        Initialize the database seeder.
        
        Args:
            connection: Active SQLite database connection
        """
        self.conn = connection
        self.cursor = connection.cursor()
        
        # Track generated IDs for relationships
        self.category_ids = []
        self.customer_ids = []
        self.product_ids = []
        self.order_ids = []
        
        # Random seed for reproducibility (optional)
        random.seed(42)
        
        logger.info("DatabaseSeeder initialized")
    
    def populate_all(self, 
                    num_customers: int = 100,
                    num_products: int = 200,
                    num_orders: int = 500):
        """
        Populate all tables with sample data.
        
        Args:
            num_customers: Number of customers to create
            num_products: Number of products to create  
            num_orders: Number of orders to create
        """
        logger.info("Starting database population...")
        
        try:
            # Order matters due to foreign key constraints
            self.populate_categories()
            self.populate_customers(num_customers)
            self.populate_products(num_products)
            self.populate_orders(num_orders)
            self.populate_product_reviews()
            self.populate_carts()
            self.populate_inventory_logs()
            
            # Commit all changes
            self.conn.commit()
            
            # Display summary
            self._display_summary()
            
            logger.info("Database population completed successfully")
            
        except Exception as e:
            logger.error(f"Error populating database: {e}")
            self.conn.rollback()
            raise
    
    def populate_categories(self):
        """
        Populate the categories table with a realistic category hierarchy.
        """
        logger.info("Populating categories...")
        
        # Main categories with their subcategories
        categories_data = [
            # Main categories (parent_id = None)
            ('Electronics', 'Electronic devices and accessories', None),
            ('Clothing', 'Fashion and apparel for all', None),
            ('Books', 'Physical and digital books', None),
            ('Home & Garden', 'Everything for your home and garden', None),
            ('Sports & Outdoors', 'Sporting goods and outdoor equipment', None),
            ('Toys & Games', 'Toys and games for all ages', None),
            ('Beauty & Health', 'Beauty products and health items', None),
            ('Food & Grocery', 'Food items and grocery products', None),
            ('Automotive', 'Auto parts and accessories', None),
            ('Office Supplies', 'Office and school supplies', None),
        ]
        
        # Insert main categories
        for category_name, description, parent_id in categories_data:
            self.cursor.execute('''
                INSERT INTO categories (category_name, description, parent_category_id)
                VALUES (?, ?, ?)
            ''', (category_name, description, parent_id))
        
        # Get the inserted category IDs
        self.cursor.execute("SELECT category_id, category_name FROM categories WHERE parent_category_id IS NULL")
        main_categories = dict(self.cursor.fetchall())
        
        # Add subcategories
        subcategories_data = [
            # Electronics subcategories
            ('Smartphones', 'Mobile phones and accessories', 'Electronics'),
            ('Laptops', 'Laptop computers', 'Electronics'),
            ('Headphones', 'Audio headphones and earbuds', 'Electronics'),
            ('Tablets', 'Tablet devices', 'Electronics'),
            ('Cameras', 'Digital cameras and accessories', 'Electronics'),
            
            # Clothing subcategories
            ('Men\'s Clothing', 'Clothing for men', 'Clothing'),
            ('Women\'s Clothing', 'Clothing for women', 'Clothing'),
            ('Kids\' Clothing', 'Clothing for children', 'Clothing'),
            ('Shoes', 'Footwear for all', 'Clothing'),
            ('Accessories', 'Fashion accessories', 'Clothing'),
            
            # Books subcategories
            ('Fiction', 'Fiction books', 'Books'),
            ('Non-Fiction', 'Non-fiction books', 'Books'),
            ('Technical', 'Technical and educational books', 'Books'),
            ('Children\'s Books', 'Books for children', 'Books'),
            
            # Home & Garden subcategories
            ('Furniture', 'Home furniture', 'Home & Garden'),
            ('Kitchen', 'Kitchen appliances and tools', 'Home & Garden'),
            ('Decor', 'Home decoration items', 'Home & Garden'),
            ('Garden Tools', 'Gardening equipment', 'Home & Garden'),
        ]
        
        # Insert subcategories
        for subcat_name, description, parent_name in subcategories_data:
            parent_id = next((k for k, v in main_categories.items() if v == parent_name), None)
            if parent_id:
                self.cursor.execute('''
                    INSERT INTO categories (category_name, description, parent_category_id)
                    VALUES (?, ?, ?)
                ''', (subcat_name, description, parent_id))
        
        # Store all category IDs for later use
        self.cursor.execute("SELECT category_id FROM categories")
        self.category_ids = [row[0] for row in self.cursor.fetchall()]
        
        logger.info(f"Created {len(self.category_ids)} categories")
    
    def populate_customers(self, num_customers: int = 100):
        """
        Populate the customers table with realistic customer data.
        
        Args:
            num_customers: Number of customers to create
        """
        logger.info(f"Populating {num_customers} customers...")
        
        # Sample data for generating customers
        first_names = [
            'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
            'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
            'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher', 'Nancy', 'Daniel', 'Lisa',
            'Matthew', 'Betty', 'Anthony', 'Dorothy', 'Donald', 'Sandra', 'Mark', 'Ashley',
            'Paul', 'Kimberly', 'Steven', 'Donna', 'Andrew', 'Emily', 'Kenneth', 'Michelle'
        ]
        
        last_names = [
            'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
            'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
            'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
            'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker'
        ]
        
        cities = [
            ('New York', 'NY', '10001'),
            ('Los Angeles', 'CA', '90001'),
            ('Chicago', 'IL', '60601'),
            ('Houston', 'TX', '77001'),
            ('Phoenix', 'AZ', '85001'),
            ('Philadelphia', 'PA', '19101'),
            ('San Antonio', 'TX', '78201'),
            ('San Diego', 'CA', '92101'),
            ('Dallas', 'TX', '75201'),
            ('San Jose', 'CA', '95101'),
            ('Austin', 'TX', '78701'),
            ('Jacksonville', 'FL', '32099'),
            ('San Francisco', 'CA', '94101'),
            ('Columbus', 'OH', '43085'),
            ('Indianapolis', 'IN', '46201'),
            ('Seattle', 'WA', '98101'),
            ('Denver', 'CO', '80201'),
            ('Boston', 'MA', '02101'),
            ('Nashville', 'TN', '37201'),
            ('Portland', 'OR', '97201')
        ]
        
        customer_types = ['regular', 'regular', 'regular', 'premium', 'vip']  # Weighted towards regular
        payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay']
        
        for i in range(num_customers):
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@example.com"
            phone = f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            
            # Simple password hash (in production, use proper hashing like bcrypt)
            password_hash = hashlib.sha256(f"password{i}".encode()).hexdigest()
            
            city, state, zip_code = random.choice(cities)
            address = f"{random.randint(1, 9999)} {random.choice(['Main', 'Oak', 'Elm', 'Maple', 'Cedar'])} {random.choice(['St', 'Ave', 'Rd', 'Blvd', 'Ln'])}"
            
            # Random date of birth (18-80 years old)
            today = date.today()
            age = random.randint(18, 80)
            dob = today - timedelta(days=age*365 + random.randint(0, 364))
            
            gender = random.choice(['M', 'F', 'Other', None])
            customer_type = random.choice(customer_types)
            
            # VIP customers get more loyalty points
            if customer_type == 'vip':
                loyalty_points = random.randint(1000, 5000)
            elif customer_type == 'premium':
                loyalty_points = random.randint(100, 1000)
            else:
                loyalty_points = random.randint(0, 100)
            
            # Customer creation date (within last 3 years)
            created_days_ago = random.randint(0, 1095)
            created_at = datetime.now() - timedelta(days=created_days_ago)
            
            # Last login (recent for active customers)
            if random.random() > 0.3:  # 70% are active
                last_login = datetime.now() - timedelta(days=random.randint(0, 30))
            else:
                last_login = None
            
            self.cursor.execute('''
                INSERT INTO customers (
                    first_name, last_name, email, phone, password_hash,
                    address, city, state, zip_code, country,
                    date_of_birth, gender, loyalty_points, customer_type,
                    preferred_payment_method, created_at, last_login_at,
                    email_verified, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                first_name, last_name, email, phone, password_hash,
                address, city, state, zip_code, 'USA',
                dob, gender, loyalty_points, customer_type,
                random.choice(payment_methods), created_at, last_login,
                random.random() > 0.1,  # 90% have verified email
                random.random() > 0.05  # 95% are active
            ))
        
        # Store customer IDs for later use
        self.cursor.execute("SELECT customer_id FROM customers")
        self.customer_ids = [row[0] for row in self.cursor.fetchall()]
        
        logger.info(f"Created {len(self.customer_ids)} customers")
    
    def populate_products(self, num_products: int = 200):
        """
        Populate the products table with realistic product data.
        
        Args:
            num_products: Number of products to create
        """
        logger.info(f"Populating {num_products} products...")
        
        # Product templates by category
        product_templates = {
            'Smartphones': [
                ('iPhone 15 Pro Max', 'Apple', 1299.99, 'Latest Apple flagship smartphone'),
                ('Samsung Galaxy S24 Ultra', 'Samsung', 1199.99, 'Premium Android smartphone'),
                ('Google Pixel 8 Pro', 'Google', 999.99, 'Google\'s flagship with AI features'),
                ('OnePlus 12', 'OnePlus', 799.99, 'Flagship killer smartphone'),
                ('iPhone 15', 'Apple', 899.99, 'Standard Apple smartphone'),
            ],
            'Laptops': [
                ('MacBook Pro 16"', 'Apple', 2499.99, 'Professional laptop for creators'),
                ('Dell XPS 15', 'Dell', 1799.99, 'Premium Windows ultrabook'),
                ('ThinkPad X1 Carbon', 'Lenovo', 1599.99, 'Business laptop'),
                ('HP Spectre x360', 'HP', 1399.99, 'Convertible laptop'),
                ('ASUS ROG Zephyrus', 'ASUS', 1999.99, 'Gaming laptop'),
            ],
            'Headphones': [
                ('AirPods Pro', 'Apple', 249.99, 'Premium wireless earbuds'),
                ('Sony WH-1000XM5', 'Sony', 399.99, 'Noise-canceling headphones'),
                ('Bose QuietComfort 45', 'Bose', 329.99, 'Comfortable ANC headphones'),
                ('Sennheiser Momentum 4', 'Sennheiser', 379.99, 'Audiophile wireless headphones'),
                ('JBL Tune 750BTNC', 'JBL', 129.99, 'Budget ANC headphones'),
            ],
            'Men\'s Clothing': [
                ('Cotton T-Shirt', 'Nike', 29.99, 'Comfortable cotton t-shirt'),
                ('Denim Jeans', 'Levis', 79.99, 'Classic fit jeans'),
                ('Polo Shirt', 'Ralph Lauren', 89.99, 'Classic polo shirt'),
                ('Hoodie', 'Adidas', 59.99, 'Warm hoodie'),
                ('Dress Shirt', 'Calvin Klein', 69.99, 'Formal dress shirt'),
            ],
            'Women\'s Clothing': [
                ('Summer Dress', 'Zara', 49.99, 'Light summer dress'),
                ('Yoga Pants', 'Lululemon', 98.00, 'High-quality yoga pants'),
                ('Blouse', 'H&M', 34.99, 'Elegant blouse'),
                ('Cardigan', 'Gap', 54.99, 'Cozy cardigan'),
                ('Skinny Jeans', 'American Eagle', 69.99, 'Trendy skinny jeans'),
            ],
            'Fiction': [
                ('The Great Gatsby', 'Scribner', 14.99, 'Classic American novel'),
                ('1984', 'Penguin', 15.99, 'Dystopian masterpiece'),
                ('Pride and Prejudice', 'Vintage', 12.99, 'Jane Austen classic'),
                ('The Hobbit', 'Mariner', 16.99, 'Fantasy adventure'),
                ('To Kill a Mockingbird', 'Harper', 14.99, 'American classic'),
            ],
            'Kitchen': [
                ('Instant Pot', 'Instant Brands', 89.99, 'Multi-functional pressure cooker'),
                ('Air Fryer', 'Ninja', 129.99, 'Healthy cooking appliance'),
                ('Coffee Maker', 'Keurig', 149.99, 'Single-serve coffee maker'),
                ('Blender', 'Vitamix', 349.99, 'High-performance blender'),
                ('Toaster Oven', 'Breville', 199.99, 'Convection toaster oven'),
            ],
            'Furniture': [
                ('Office Chair', 'Herman Miller', 899.99, 'Ergonomic office chair'),
                ('Standing Desk', 'Flexispot', 499.99, 'Adjustable standing desk'),
                ('Bookshelf', 'IKEA', 149.99, '5-tier bookshelf'),
                ('Coffee Table', 'West Elm', 399.99, 'Modern coffee table'),
                ('Sofa', 'Article', 1299.99, '3-seater modern sofa'),
            ],
        }
        
        colors = ['Black', 'White', 'Silver', 'Gold', 'Blue', 'Red', 'Green', 'Gray', 'Navy', 'Rose Gold']
        sizes = ['XS', 'S', 'M', 'L', 'XL', 'XXL', None]
        materials = ['Cotton', 'Polyester', 'Leather', 'Metal', 'Plastic', 'Wood', 'Glass', None]
        
        products_created = 0
        
        # Get all leaf categories (categories without children)
        self.cursor.execute('''
            SELECT c1.category_id, c1.category_name 
            FROM categories c1
            WHERE NOT EXISTS (
                SELECT 1 FROM categories c2 
                WHERE c2.parent_category_id = c1.category_id
            )
        ''')
        leaf_categories = dict(self.cursor.fetchall())
        
        for _ in range(num_products):
            # Select a random category
            category_id = random.choice(list(leaf_categories.keys()))
            category_name = leaf_categories[category_id]
            
            # Get product template for this category
            if category_name in product_templates:
                template = random.choice(product_templates[category_name])
                base_name, brand, base_price, description = template
                
                # Add variations
                color = random.choice(colors) if random.random() > 0.3 else None
                size = random.choice(sizes) if category_name in ['Men\'s Clothing', 'Women\'s Clothing'] else None
                
                # Construct product name with variations
                product_name = base_name
                if color:
                    product_name = f"{color} {product_name}"
                if size:
                    product_name = f"{product_name} - Size {size}"
                
                # Add some price variation
                price = base_price * random.uniform(0.8, 1.2)
                
            else:
                # Generic product for categories without templates
                product_name = f"{category_name} Item {products_created + 1}"
                brand = random.choice(['Generic', 'BrandX', 'Premium Co', 'Value Brand'])
                price = random.uniform(9.99, 299.99)
                description = f"Quality product in {category_name} category"
                color = random.choice(colors) if random.random() > 0.5 else None
                size = None
            
            # Generate SKU
            sku = f"SKU-{category_id:03d}-{products_created:05d}"
            
            # Cost (60-80% of price for margin calculation)
            cost = price * random.uniform(0.6, 0.8)
            
            # Stock levels
            stock_quantity = random.randint(0, 500)
            reserved_quantity = min(random.randint(0, 20), stock_quantity)
            reorder_level = random.randint(10, 50)
            
            # Product attributes
            weight = random.uniform(0.1, 10.0) if random.random() > 0.3 else None
            dimensions = f"{random.randint(5,50)}x{random.randint(5,50)}x{random.randint(5,50)}" if random.random() > 0.5 else None
            
            # Features and status
            is_featured = random.random() > 0.9  # 10% are featured
            is_active = random.random() > 0.05  # 95% are active
            discount_percentage = random.choice([0, 0, 0, 5, 10, 15, 20, 25])  # Most have no discount
            tax_rate = random.choice([0, 6.5, 7.0, 8.5, 9.0])  # Various tax rates
            
            # Ratings (products with more history have ratings)
            if random.random() > 0.3:  # 70% have ratings
                rating_count = random.randint(1, 500)
                rating_average = random.uniform(3.0, 5.0)
            else:
                rating_count = 0
                rating_average = 0
            
            # View count (popular products have more views)
            view_count = random.randint(0, 10000)
            
            # Launch date (within last 2 years)
            launch_date = date.today() - timedelta(days=random.randint(0, 730))
            
            self.cursor.execute('''
                INSERT INTO products (
                    product_name, category_id, sku, price, cost,
                    stock_quantity, reserved_quantity, reorder_level,
                    description, brand, weight, dimensions,
                    color, size, material,
                    is_active, is_featured, discount_percentage, tax_rate,
                    rating_average, rating_count, view_count,
                    launch_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                product_name, category_id, sku, round(price, 2), round(cost, 2),
                stock_quantity, reserved_quantity, reorder_level,
                description, brand, weight, dimensions,
                color, size, random.choice(materials),
                is_active, is_featured, discount_percentage, tax_rate,
                round(rating_average, 2), rating_count, view_count,
                launch_date
            ))
            
            products_created += 1
        
        # Store product IDs for later use
        self.cursor.execute("SELECT product_id FROM products")
        self.product_ids = [row[0] for row in self.cursor.fetchall()]
        
        logger.info(f"Created {len(self.product_ids)} products")
    
    def populate_orders(self, num_orders: int = 500):
        """
        Populate orders and order_items tables with realistic order data.
        
        Args:
            num_orders: Number of orders to create
        """
        logger.info(f"Populating {num_orders} orders...")
        
        order_statuses = ['pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled']
        payment_statuses = ['pending', 'paid', 'failed', 'refunded']
        payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay', 'bank_transfer']
        shipping_methods = ['standard', 'express', 'overnight']
        
        for order_num in range(num_orders):
            # Select a random customer
            customer_id = random.choice(self.customer_ids)
            
            # Get customer info for shipping address
            self.cursor.execute('''
                SELECT address, city, state, zip_code, country, preferred_payment_method
                FROM customers WHERE customer_id = ?
            ''', (customer_id,))
            customer_info = self.cursor.fetchone()
            
            # Order date (within last 6 months)
            days_ago = random.randint(0, 180)
            order_date = datetime.now() - timedelta(days=days_ago)
            
            # Order number (human-readable)
            order_number = f"ORD-{order_date.strftime('%Y%m')}-{order_num:05d}"
            
            # Status based on age of order
            if days_ago < 7:
                status = random.choice(['pending', 'confirmed', 'processing', 'shipped'])
            elif days_ago < 30:
                status = random.choice(['shipped', 'delivered', 'delivered', 'delivered'])
            else:
                status = random.choice(['delivered', 'delivered', 'delivered', 'cancelled'])
            
            # Payment status correlates with order status
            if status in ['delivered', 'shipped', 'processing']:
                payment_status = 'paid'
            elif status == 'cancelled':
                payment_status = random.choice(['refunded', 'failed'])
            else:
                payment_status = random.choice(['pending', 'paid'])
            
            # Dates based on status
            required_date = order_date + timedelta(days=random.randint(3, 10))
            shipped_date = None
            delivered_date = None
            
            if status in ['shipped', 'delivered']:
                shipped_date = order_date + timedelta(days=random.randint(1, 3))
            
            if status == 'delivered':
                delivered_date = shipped_date + timedelta(days=random.randint(1, 5))
            
            # Payment and shipping
            payment_method = customer_info[5] or random.choice(payment_methods)
            shipping_method = random.choice(shipping_methods)
            
            # Shipping cost based on method
            shipping_costs = {'standard': 5.99, 'express': 12.99, 'overnight': 29.99}
            shipping_cost = shipping_costs[shipping_method]
            
            # Tracking number for shipped orders
            tracking_number = f"TRK{random.randint(100000000, 999999999)}" if shipped_date else None
            
            # Insert order (totals will be updated after adding items)
            self.cursor.execute('''
                INSERT INTO orders (
                    customer_id, order_number, order_date, required_date,
                    shipped_date, delivered_date, status, payment_status,
                    payment_method, shipping_method, shipping_cost,
                    shipping_address, shipping_city, shipping_state,
                    shipping_zip, shipping_country,
                    billing_address, billing_city, billing_state,
                    billing_zip, billing_country,
                    tracking_number, subtotal, tax_amount, 
                    discount_amount, total_amount
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0)
            ''', (
                customer_id, order_number, order_date, required_date,
                shipped_date, delivered_date, status, payment_status,
                payment_method, shipping_method, shipping_cost,
                customer_info[0], customer_info[1], customer_info[2],
                customer_info[3], customer_info[4],
                customer_info[0], customer_info[1], customer_info[2],
                customer_info[3], customer_info[4],
                tracking_number
            ))
            
            order_id = self.cursor.lastrowid
            self.order_ids.append(order_id)
            
            # Add order items (1-10 items per order)
            num_items = random.randint(1, 10)
            selected_products = random.sample(self.product_ids, min(num_items, len(self.product_ids)))
            
            subtotal = 0
            tax_amount = 0
            discount_amount = 0
            
            for product_id in selected_products:
                # Get product info
                self.cursor.execute('''
                    SELECT price, discount_percentage, tax_rate
                    FROM products WHERE product_id = ?
                ''', (product_id,))
                product_info = self.cursor.fetchone()
                
                if not product_info:
                    continue
                
                unit_price = product_info[0]
                discount_pct = product_info[1] or 0
                tax_rate = product_info[2] or 0
                
                # Quantity (usually 1-3)
                quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
                
                # Calculate amounts
                item_discount = (unit_price * quantity * discount_pct / 100)
                item_subtotal = (unit_price * quantity) - item_discount
                item_tax = item_subtotal * (tax_rate / 100)
                item_total = item_subtotal + item_tax
                
                # Insert order item
                self.cursor.execute('''
                    INSERT INTO order_items (
                        order_id, product_id, quantity, unit_price,
                        discount_amount, tax_amount, subtotal, total,
                        fulfillment_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    order_id, product_id, quantity, unit_price,
                    round(item_discount, 2), round(item_tax, 2),
                    round(item_subtotal, 2), round(item_total, 2),
                    'delivered' if status == 'delivered' else 'pending'
                ))
                
                subtotal += item_subtotal
                tax_amount += item_tax
                discount_amount += item_discount
            
            # Update order totals
            total_amount = subtotal + tax_amount + shipping_cost
            
            self.cursor.execute('''
                UPDATE orders 
                SET subtotal = ?, tax_amount = ?, discount_amount = ?, total_amount = ?
                WHERE order_id = ?
            ''', (
                round(subtotal, 2), round(tax_amount, 2),
                round(discount_amount, 2), round(total_amount, 2),
                order_id
            ))
        
        logger.info(f"Created {len(self.order_ids)} orders with items")
    
    def populate_product_reviews(self):
        """
        Populate the product_reviews table with realistic review data.
        """
        logger.info("Populating product reviews...")
        
        review_titles = [
            'Excellent product!', 'Good value for money', 'Not what I expected',
            'Highly recommend', 'Decent quality', 'Love it!', 'Disappointed',
            'Better than expected', 'Worth every penny', 'Just okay'
        ]
        
        positive_comments = [
            'Great quality and fast shipping. Would buy again!',
            'Exactly as described. Very happy with my purchase.',
            'Excellent product, exceeded my expectations.',
            'Perfect! Just what I was looking for.',
            'Amazing quality at this price point.'
        ]
        
        neutral_comments = [
            'Product is okay, nothing special but does the job.',
            'Average quality, you get what you pay for.',
            'It\'s fine, but I\'ve seen better.',
            'Meets basic expectations, nothing more.',
            'Acceptable quality for the price.'
        ]
        
        negative_comments = [
            'Product broke after a week of use.',
            'Not worth the money. Very disappointed.',
            'Quality is much worse than expected.',
            'Would not recommend. Look elsewhere.',
            'Received a defective item. Poor quality control.'
        ]
        
        # Get all delivered orders with their products
        self.cursor.execute('''
            SELECT DISTINCT oi.order_id, oi.product_id, o.customer_id
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.status = 'delivered'
            LIMIT 200
        ''')
        
        delivered_items = self.cursor.fetchall()
        
        reviews_created = 0
        for order_id, product_id, customer_id in delivered_items:
            # Not everyone leaves reviews (30% chance)
            if random.random() > 0.3:
                continue
            
            # Check if review already exists
            self.cursor.execute('''
                SELECT COUNT(*) FROM product_reviews 
                WHERE product_id = ? AND customer_id = ? AND order_id = ?
            ''', (product_id, customer_id, order_id))
            
            if self.cursor.fetchone()[0] > 0:
                continue
            
            # Generate rating (weighted towards positive)
            rating = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 15, 30, 40])[0]
            
            # Select appropriate comments based on rating
            if rating >= 4:
                title = random.choice(['Excellent product!', 'Highly recommend', 'Love it!', 'Worth every penny'])
                comment = random.choice(positive_comments)
                is_recommended = True
            elif rating == 3:
                title = random.choice(['Decent quality', 'Just okay', 'Good value for money'])
                comment = random.choice(neutral_comments)
                is_recommended = random.random() > 0.5
            else:
                title = random.choice(['Not what I expected', 'Disappointed', 'Poor quality'])
                comment = random.choice(negative_comments)
                is_recommended = False
            
            # Helpful votes (more for older reviews)
            helpful_count = random.randint(0, 50) if random.random() > 0.5 else 0
            not_helpful_count = random.randint(0, 10) if helpful_count > 0 else 0
            
            # Review status (most are approved)
            status = random.choices(['approved', 'pending', 'rejected'], weights=[85, 10, 5])[0]
            
            # Created date (after order delivery)
            self.cursor.execute('SELECT delivered_date FROM orders WHERE order_id = ?', (order_id,))
            delivered_date = self.cursor.fetchone()[0]
            if delivered_date:
                review_date = datetime.fromisoformat(delivered_date) + timedelta(days=random.randint(1, 30))
            else:
                review_date = datetime.now() - timedelta(days=random.randint(1, 90))
            
            self.cursor.execute('''
                INSERT INTO product_reviews (
                    product_id, customer_id, order_id, rating,
                    title, comment, is_verified_purchase, is_recommended,
                    helpful_count, not_helpful_count, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                product_id, customer_id, order_id, rating,
                title, comment, True, is_recommended,
                helpful_count, not_helpful_count, status, review_date
            ))
            
            reviews_created += 1
            
            # Update product rating average
            self.cursor.execute('''
                UPDATE products 
                SET rating_average = (
                    SELECT AVG(rating) FROM product_reviews 
                    WHERE product_id = ? AND status = 'approved'
                ),
                rating_count = (
                    SELECT COUNT(*) FROM product_reviews 
                    WHERE product_id = ? AND status = 'approved'
                )
                WHERE product_id = ?
            ''', (product_id, product_id, product_id))
        
        logger.info(f"Created {reviews_created} product reviews")
    
    def populate_carts(self):
        """
        Populate cart and cart_items tables with active and abandoned carts.
        """
        logger.info("Populating shopping carts...")
        
        carts_created = 0
        
        # Create carts for 30% of customers
        sample_customers = random.sample(self.customer_ids, int(len(self.customer_ids) * 0.3))
        
        for customer_id in sample_customers:
            # Cart status (most are active)
            status = random.choices(['active', 'abandoned', 'converted'], weights=[60, 30, 10])[0]
            
            # Cart age
            if status == 'active':
                created_at = datetime.now() - timedelta(hours=random.randint(1, 72))
            elif status == 'abandoned':
                created_at = datetime.now() - timedelta(days=random.randint(3, 30))
            else:  # converted
                created_at = datetime.now() - timedelta(days=random.randint(1, 60))
            
            expires_at = created_at + timedelta(days=30)
            
            self.cursor.execute('''
                INSERT INTO cart (customer_id, status, created_at, updated_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (customer_id, status, created_at, created_at, expires_at))
            
            cart_id = self.cursor.lastrowid
            
            # Add items to cart (1-5 items)
            num_items = random.randint(1, 5)
            selected_products = random.sample(self.product_ids, min(num_items, len(self.product_ids)))
            
            for product_id in selected_products:
                quantity = random.choices([1, 2, 3], weights=[70, 20, 10])[0]
                saved_for_later = random.random() > 0.9  # 10% saved for later
                
                self.cursor.execute('''
                    INSERT INTO cart_items (cart_id, product_id, quantity, saved_for_later, added_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (cart_id, product_id, quantity, saved_for_later, created_at))
                
                # Update product reserved quantity for active carts
                if status == 'active' and not saved_for_later:
                    self.cursor.execute('''
                        UPDATE products 
                        SET reserved_quantity = reserved_quantity + ?
                        WHERE product_id = ?
                    ''', (quantity, product_id))
            
            carts_created += 1
        
        # Create some anonymous carts (with session_id only)
        for _ in range(20):
            session_id = f"session_{random.randint(100000, 999999)}"
            created_at = datetime.now() - timedelta(hours=random.randint(1, 168))
            expires_at = created_at + timedelta(days=7)  # Anonymous carts expire faster
            
            self.cursor.execute('''
                INSERT INTO cart (session_id, status, created_at, updated_at, expires_at)
                VALUES (?, 'abandoned', ?, ?, ?)
            ''', (session_id, created_at, created_at, expires_at))
            
            cart_id = self.cursor.lastrowid
            
            # Add 1-3 items
            num_items = random.randint(1, 3)
            selected_products = random.sample(self.product_ids, min(num_items, len(self.product_ids)))
            
            for product_id in selected_products:
                self.cursor.execute('''
                    INSERT INTO cart_items (cart_id, product_id, quantity, added_at)
                    VALUES (?, ?, 1, ?)
                ''', (cart_id, product_id, created_at))
            
            carts_created += 1
        
        logger.info(f"Created {carts_created} shopping carts")
    
    def populate_inventory_logs(self):
        """
        Populate inventory_log table with historical inventory movements.
        """
        logger.info("Populating inventory logs...")
        
        change_types = ['purchase', 'sale', 'return', 'adjustment', 'damage', 'restock']
        
        logs_created = 0
        
        # Create logs for random products
        sample_products = random.sample(self.product_ids, min(50, len(self.product_ids)))
        
        for product_id in sample_products:
            # Get current stock
            self.cursor.execute('SELECT stock_quantity FROM products WHERE product_id = ?', (product_id,))
            current_stock = self.cursor.fetchone()[0]
            
            # Generate 3-10 historical events
            num_events = random.randint(3, 10)
            running_stock = current_stock
            
            for i in range(num_events):
                change_type = random.choice(change_types)
                
                # Determine quantity change based on type
                if change_type == 'purchase':
                    quantity_change = random.randint(50, 200)
                elif change_type == 'sale':
                    quantity_change = -random.randint(1, 20)
                elif change_type == 'return':
                    quantity_change = random.randint(1, 5)
                elif change_type == 'adjustment':
                    quantity_change = random.randint(-10, 10)
                elif change_type == 'damage':
                    quantity_change = -random.randint(1, 5)
                else:  # restock
                    quantity_change = random.randint(20, 100)
                
                # Calculate before and after
                quantity_before = running_stock - quantity_change
                quantity_after = running_stock
                
                # Don't let stock go negative
                if quantity_before < 0:
                    quantity_before = 0
                    quantity_change = quantity_after - quantity_before
                
                # Create log entry
                log_date = datetime.now() - timedelta(days=random.randint(1, 180))
                
                self.cursor.execute('''
                    INSERT INTO inventory_log (
                        product_id, change_type, quantity_change,
                        quantity_before, quantity_after, notes,
                        performed_by, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    product_id, change_type, quantity_change,
                    quantity_before, quantity_after,
                    f"{change_type.capitalize()} inventory adjustment",
                    'System Admin', log_date
                ))
                
                running_stock = quantity_before
                logs_created += 1
        
        logger.info(f"Created {logs_created} inventory log entries")
    
    def _display_summary(self):
        """
        Display a summary of the populated data.
        """
        print("\n" + "="*60)
        print("DATABASE POPULATION SUMMARY")
        print("="*60)
        
        tables = [
            'categories', 'customers', 'products', 'orders',
            'order_items', 'product_reviews', 'cart', 'cart_items',
            'inventory_log'
        ]
        
        for table in tables:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = self.cursor.fetchone()[0]
            print(f"{table.capitalize():20} {count:,} records")
        
        # Additional statistics
        print("\n" + "-"*60)
        print("BUSINESS METRICS")
        print("-"*60)
        
        # Total revenue
        self.cursor.execute("""
            SELECT SUM(total_amount) FROM orders 
            WHERE status NOT IN ('cancelled', 'refunded')
        """)
        total_revenue = self.cursor.fetchone()[0] or 0
        print(f"Total Revenue:       ${total_revenue:,.2f}")
        
        # Average order value
        self.cursor.execute("""
            SELECT AVG(total_amount) FROM orders 
            WHERE status NOT IN ('cancelled', 'refunded')
        """)
        avg_order = self.cursor.fetchone()[0] or 0
        print(f"Average Order Value: ${avg_order:,.2f}")
        
        # Top selling category
        self.cursor.execute("""
            SELECT c.category_name, COUNT(*) as sales
            FROM order_items oi
            JOIN products p ON oi.product_id = p.product_id
            JOIN categories c ON p.category_id = c.category_id
            GROUP BY c.category_id
            ORDER BY sales DESC
            LIMIT 1
        """)
        top_category = self.cursor.fetchone()
        if top_category:
            print(f"Top Category:        {top_category[0]} ({top_category[1]} items sold)")
        
        # Customer retention
        self.cursor.execute("""
            SELECT COUNT(DISTINCT customer_id) FROM orders
        """)
        customers_with_orders = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM customers")
        total_customers = self.cursor.fetchone()[0]
        
        if total_customers > 0:
            retention_rate = (customers_with_orders / total_customers) * 100
            print(f"Customer Retention:  {retention_rate:.1f}%")
        
        print("="*60 + "\n")


def main():
    """
    Main function to populate the database when running this module directly.
    """
    import sys
    import os
    
    # Add parent directory to path
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from database.creator import DatabaseCreator
    
    # Create database first
    print("Creating database schema...")
    with DatabaseCreator('data/ecommerce.db') as creator:
        conn = creator.create_database()
        
        # Populate with sample data
        print("\nPopulating database with sample data...")
        seeder = DatabaseSeeder(conn)
        
        # You can adjust these numbers based on your needs
        seeder.populate_all(
            num_customers=100,
            num_products=200,
            num_orders=500
        )
        
        print("\n✅ Database successfully created and populated!")
        print("   Location: data/ecommerce.db")


if __name__ == "__main__":
    main()